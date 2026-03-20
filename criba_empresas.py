# ============================================================================
# CRIBA DE GANGAS GENERACIONALES v2 - Deep Value Screening (MASIVO)
# Basado en el documento "criba (1).pdf"
# Autor: Quant Developer
# Fecha: 2026-02-08
# ============================================================================
# CAMBIOS v2.1:
#   - Descarga automática de tickers S&P500 desde Wikipedia
#   - Soporte para companylist.csv (NASDAQ/NYSE completo)
#   - Barra de progreso con tqdm
#   - Anti-bloqueo con time.sleep entre peticiones
#   - Manejo de errores silencioso para procesamiento masivo
# ============================================================================

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import time
import warnings
warnings.filterwarnings("ignore")

try:
    from tqdm import tqdm
except ImportError:
    print("⚠️  Instalando tqdm...")
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tqdm", "-q"])
    from tqdm import tqdm

# ============================================================================
# CONFIGURACIÓN GLOBAL
# ============================================================================
DELAY_ENTRE_PETICIONES = 0.5        # Segundos entre cada ticker (anti-bloqueo)
CSV_LOCAL = "companylist.csv"       # Archivo local opcional (NASDAQ/NYSE)
CSV_SALIDA = "gangas_generacionales_v2.csv"
MAX_REINTENTOS = 2                  # Reintentos por ticker si falla la conexión

# ============================================================================
# CARGA DINÁMICA DE TICKERS (reemplaza lista manual)
# ============================================================================

def get_sp500_tickers():
    """
    Descarga automáticamente la lista actual del S&P 500 desde Wikipedia.
    Fuente: https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
    """
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tablas = pd.read_html(url)
        df_sp500 = tablas[0]
        tickers = df_sp500["Symbol"].str.strip().str.replace(".", "-", regex=False).tolist()
        return tickers
    except Exception:
        return None


def cargar_companylist_csv():
    """
    Carga tickers desde un archivo local 'companylist.csv'.
    Formato estándar NASDAQ/NYSE: columna 'Symbol' o 'Ticker'.
    Si no existe el archivo, retorna None.
    """
    ruta = Path(CSV_LOCAL)
    if not ruta.exists():
        return None
    
    try:
        df = pd.read_csv(ruta)
        # Buscar la columna de tickers (varios nombres posibles)
        col_ticker = None
        for candidata in ["Symbol", "symbol", "Ticker", "ticker", "SYMBOL", "TICKER"]:
            if candidata in df.columns:
                col_ticker = candidata
                break
        
        if col_ticker is None:
            # Si no encuentra nombre, usa la primera columna
            col_ticker = df.columns[0]
        
        tickers = df[col_ticker].dropna().astype(str).str.strip().tolist()
        # Filtrar tickers inválidos
        tickers = [t for t in tickers if t.isalpha() or "-" in t or "." in t]
        return tickers if tickers else None
    except Exception:
        return None


def obtener_tickers(extra_tickers=None):
    """
    Pipeline de carga de tickers:
    1. Si existe companylist.csv → usa toda la bolsa
    2. Si no → descarga S&P 500 de Wikipedia
    3. Si falla todo → lista de emergencia hardcoded
    """
    tickers_finales = []
    fuente = ""
    # Opción 1: Archivo local con toda la bolsa
    tickers_csv = cargar_companylist_csv()
    if tickers_csv:
        print(f"  📂 Cargados {len(tickers_csv)} tickers desde '{CSV_LOCAL}'")
        tickers_finales = tickers_csv
        fuente = "companylist.csv"
    else:
        # Opción 2: S&P 500 desde Wikipedia
        print("  🌐 Descargando lista S&P 500 desde Wikipedia...")
        tickers_sp500 = get_sp500_tickers()
        if tickers_sp500:
            print(f"  ✅ Cargados {len(tickers_sp500)} tickers del S&P 500")
            tickers_finales = tickers_sp500
            fuente = "S&P 500 (Wikipedia)"
        else:
            # Opción 3: Lista de emergencia
            print("  ⚠️  Usando lista de emergencia (30 tickers)")
            emergencia = [
                "AAPL", "MSFT", "GOOG", "META", "NVDA", "JPM", "BAC", "XOM",
                "CVX", "JNJ", "PG", "KO", "WMT", "CAT", "GE", "HON", "IBM",
                "INTC", "CSCO", "PFE", "MRK", "UNH", "HD", "MCD", "BA",
                "GS", "MS", "C", "WFC", "COP"
            ]
            tickers_finales = emergencia
            fuente = "Lista de emergencia"

    if extra_tickers:
        print(f"  ➕ Añadiendo {len(extra_tickers)} tickers extra desde la cartera actual.")
        tickers_finales = list(set(tickers_finales + extra_tickers))
        
    return tickers_finales, fuente


# ============================================================================
# MAPEO DE SECTORES (según PDF - Protocolos de Valoración por Sector, pág 2)
# ============================================================================
SECTOR_MAP = {
    "Technology":             "tecnologia",
    "Communication Services": "tecnologia",
    "Industrials":            "industrial",
    "Energy":                 "energia",
    "Basic Materials":        "energia",
    "Financial Services":     "bancos",
    "Healthcare":             "industrial",
    "Consumer Cyclical":      "industrial",
    "Consumer Defensive":     "industrial",
    "Utilities":              "industrial",
    "Real Estate":            "bancos",
}


# ============================================================================
# PASO 1: FILTROS UNIVERSALES DE CALIDAD - "EL MOTOR" (pág 1 del PDF)
# ============================================================================
# Filtro 1: ROIC > 10%
# Filtro 2: Deuda Neta / EBITDA < 3x
# Filtro 3: Interest Coverage Ratio > 3x
# Filtro 4: Piotroski F-Score >= 5
# ============================================================================

def calcular_roic(info, financials, balance):
    """
    ROIC = NOPAT / Capital Invertido
    (Filtro Universal de Calidad - ROIC > 10%, pág 1)
    """
    try:
        ebit = financials.loc["EBIT"].dropna().iloc[0]
        tax_rate = info.get("taxRate", info.get("effectiveTaxRate", 0.25))
        if tax_rate is None or tax_rate == 0:
            tax_rate = 0.25
        
        nopat = ebit * (1 - tax_rate)
        total_assets = balance.loc["Total Assets"].dropna().iloc[0]
        current_liabilities = balance.loc["Current Liabilities"].dropna().iloc[0]
        capital_invertido = total_assets - current_liabilities
        
        if capital_invertido <= 0:
            return None
        return (nopat / capital_invertido) * 100
    except Exception:
        return None


def calcular_deuda_ebitda(info, financials, balance):
    """
    Deuda Neta / EBITDA < 3x
    (Filtro Universal de Calidad - Apalancamiento, pág 1)
    """
    try:
        total_debt = info.get("totalDebt", None)
        cash = info.get("totalCash", None)
        ebitda = info.get("ebitda", None)
        
        if total_debt is None:
            total_debt = balance.loc["Total Debt"].dropna().iloc[0]
        if cash is None:
            cash = balance.loc["Cash And Cash Equivalents"].dropna().iloc[0]
        if ebitda is None:
            ebitda = financials.loc["EBITDA"].dropna().iloc[0]
        
        if ebitda is None or ebitda <= 0:
            return None
        
        return (total_debt - cash) / ebitda
    except Exception:
        return None


def calcular_interest_coverage(financials):
    """
    Interest Coverage = EBIT / |Interest Expense| > 3x
    (Filtro Universal de Calidad - Cobertura de intereses, pág 1)
    """
    try:
        ebit = financials.loc["EBIT"].dropna().iloc[0]
        interest = abs(financials.loc["Interest Expense"].dropna().iloc[0])
        if interest == 0:
            return 999
        return ebit / interest
    except Exception:
        return None


def calcular_piotroski_fscore(financials, balance, cashflow):
    """
    Piotroski F-Score (0-9) calculado manualmente.
    Se requiere F-Score >= 5 según el PDF (pág 1).
    
    Rentabilidad (4 pts): ROA>0, CFO>0, ΔROA>0, CFO>NI
    Apalancamiento (3 pts): ΔDeudaLP<0, ΔCR>0, no dilución
    Eficiencia (2 pts): ΔMargenBruto>0, ΔAssetTurnover>0
    """
    score = 0
    try:
        net_income = financials.loc["Net Income"].dropna()
        total_assets = balance.loc["Total Assets"].dropna()
        
        # 1. ROA > 0
        if len(net_income) >= 1 and len(total_assets) >= 1:
            if net_income.iloc[0] / total_assets.iloc[0] > 0:
                score += 1
        
        # 2. CFO > 0
        try:
            cfo = cashflow.loc["Operating Cash Flow"].dropna()
            if len(cfo) >= 1 and cfo.iloc[0] > 0:
                score += 1
        except Exception:
            pass
        
        # 3. Delta ROA > 0
        if len(net_income) >= 2 and len(total_assets) >= 2:
            roa_actual = net_income.iloc[0] / total_assets.iloc[0]
            roa_anterior = net_income.iloc[1] / total_assets.iloc[1]
            if roa_actual > roa_anterior:
                score += 1
        
        # 4. CFO > Net Income (calidad de beneficios)
        try:
            cfo = cashflow.loc["Operating Cash Flow"].dropna()
            if len(cfo) >= 1 and len(net_income) >= 1:
                if cfo.iloc[0] > net_income.iloc[0]:
                    score += 1
        except Exception:
            pass
        
        # 5. Deuda LP disminuye
        try:
            long_debt = balance.loc["Long Term Debt"].dropna()
            if len(long_debt) >= 2:
                if long_debt.iloc[0] <= long_debt.iloc[1]:
                    score += 1
            else:
                score += 1
        except Exception:
            score += 1
        
        # 6. Current Ratio mejora
        try:
            ca = balance.loc["Current Assets"].dropna()
            cl = balance.loc["Current Liabilities"].dropna()
            if len(ca) >= 2 and len(cl) >= 2:
                if (ca.iloc[0] / cl.iloc[0]) > (ca.iloc[1] / cl.iloc[1]):
                    score += 1
        except Exception:
            pass
        
        # 7. No dilución de acciones
        try:
            shares = balance.loc["Ordinary Shares Number"].dropna()
            if len(shares) < 2:
                shares = balance.loc["Share Issued"].dropna()
            if len(shares) >= 2 and shares.iloc[0] <= shares.iloc[1]:
                score += 1
        except Exception:
            pass
        
        # 8. Margen bruto mejora
        try:
            rev = financials.loc["Total Revenue"].dropna()
            gp = financials.loc["Gross Profit"].dropna()
            if len(rev) >= 2 and len(gp) >= 2:
                if (gp.iloc[0] / rev.iloc[0]) > (gp.iloc[1] / rev.iloc[1]):
                    score += 1
        except Exception:
            pass
        
        # 9. Asset Turnover mejora
        try:
            rev = financials.loc["Total Revenue"].dropna()
            if len(rev) >= 2 and len(total_assets) >= 2:
                if (rev.iloc[0] / total_assets.iloc[0]) > (rev.iloc[1] / total_assets.iloc[1]):
                    score += 1
        except Exception:
            pass
        
        return score
    except Exception:
        return None


def filtros_calidad(info, financials, balance, cashflow):
    """Aplica los 4 filtros universales de calidad (pág 1 del PDF)."""
    roic = calcular_roic(info, financials, balance)
    deuda_ebitda = calcular_deuda_ebitda(info, financials, balance)
    interest_cov = calcular_interest_coverage(financials)
    piotroski = calcular_piotroski_fscore(financials, balance, cashflow)
    
    cumple_todo = (
        (roic is not None and roic > 10) and
        (deuda_ebitda is not None and deuda_ebitda < 3) and
        (interest_cov is not None and interest_cov > 3) and
        (piotroski is not None and piotroski >= 5)
    )
    
    return {
        "roic": round(roic, 2) if roic else None,
        "deuda_ebitda": round(deuda_ebitda, 2) if deuda_ebitda else None,
        "interest_coverage": round(interest_cov, 2) if interest_cov else None,
        "piotroski": piotroski,
        "cumple_calidad": cumple_todo
    }


# ============================================================================
# PASO 2: FILTRO ANTI-DILUCIÓN (pág 1-2 del PDF)
# ============================================================================
# Shares Outstanding: tendencia bajista o estable.
# Dilución neta > 2% anual → DESCARTADA
# ============================================================================

def filtro_anti_dilucion(balance):
    """Filtro Anti-Dilución (pág 1-2 del PDF)."""
    try:
        shares = None
        for key in ["Ordinary Shares Number", "Share Issued", "Common Stock"]:
            try:
                s = balance.loc[key].dropna()
                if len(s) >= 2:
                    shares = s
                    break
            except Exception:
                continue
        
        if shares is None or len(shares) < 2:
            return {"dilucion_anual_pct": None, "cumple_dilucion": True}
        
        shares_reciente = shares.iloc[0]
        shares_antiguo = shares.iloc[-1]
        
        if shares_antiguo == 0:
            return {"dilucion_anual_pct": None, "cumple_dilucion": True}
        
        cambio_total = (shares_reciente - shares_antiguo) / shares_antiguo * 100
        n_years = max(len(shares) - 1, 1)
        dilucion_anual = cambio_total / n_years
        
        return {
            "dilucion_anual_pct": round(dilucion_anual, 2),
            "cumple_dilucion": dilucion_anual <= 2.0
        }
    except Exception:
        return {"dilucion_anual_pct": None, "cumple_dilucion": True}


# ============================================================================
# PASO 3: PROTOCOLOS DE VALORACIÓN POR SECTOR (pág 2 del PDF)
# ============================================================================
# Tecnología:  EV/FCF < 20, PEG < 1.5, Margen FCF > 15%
# Industrial:  EV/EBITDA < 10, P/E < 15, Dividend Yield > 2%
# Energía:     EV/EBITDA < 5, P/FCF < 8, P/Book < 1.5
# Bancos:      P/Book < 1.0, P/E < 10, ROE > 12%
#
# NOTA: "Coste de Reposición" (mineras) y "NPL Ratio" (bancos) no
# disponibles en yfinance → se usa P/Book como proxy conservador.
# ============================================================================

def obtener_sector_normalizado(info):
    """Detecta sector y mapea al esquema del PDF."""
    return SECTOR_MAP.get(info.get("sector", "Unknown"), "industrial")


def valoracion_tecnologia(info):
    """Protocolo Tecnología (pág 2): EV/FCF<20, PEG<1.5, MargenFCF>15%"""
    try:
        ev = info.get("enterpriseValue")
        fcf = info.get("freeCashflow")
        peg = info.get("pegRatio")
        revenue = info.get("totalRevenue")
        
        r = {}
        cumple = True
        
        if ev and fcf and fcf > 0:
            r["EV_FCF"] = round(ev / fcf, 2)
            if r["EV_FCF"] >= 20: cumple = False
        else:
            r["EV_FCF"] = None
        
        if peg and peg > 0:
            r["PEG"] = round(peg, 2)
            if peg >= 1.5: cumple = False
        else:
            r["PEG"] = None
        
        if fcf and revenue and revenue > 0:
            r["Margen_FCF"] = round((fcf / revenue) * 100, 2)
            if r["Margen_FCF"] <= 15: cumple = False
        else:
            r["Margen_FCF"] = None
        
        r["cumple_valoracion"] = cumple
        return r
    except Exception:
        return {"cumple_valoracion": False}


def valoracion_industrial(info):
    """Protocolo Industrial (pág 2): EV/EBITDA<10, P/E<15, DivYield>2%"""
    try:
        r = {}
        cumple = True
        
        ev_ebitda = info.get("enterpriseToEbitda")
        if ev_ebitda:
            r["EV_EBITDA"] = round(ev_ebitda, 2)
            if ev_ebitda >= 10: cumple = False
        else:
            r["EV_EBITDA"] = None
        
        pe = info.get("trailingPE", info.get("forwardPE"))
        if pe:
            r["PE"] = round(pe, 2)
            if pe >= 15: cumple = False
        else:
            r["PE"] = None
        
        div_yield = info.get("dividendYield")
        if div_yield:
            r["Div_Yield"] = round(div_yield * 100, 2)
            if r["Div_Yield"] <= 2: cumple = False
        else:
            r["Div_Yield"] = None
            cumple = False
        
        r["cumple_valoracion"] = cumple
        return r
    except Exception:
        return {"cumple_valoracion": False}


def valoracion_energia(info):
    """Protocolo Energía (pág 2): EV/EBITDA<5, P/FCF<8, P/Book<1.5"""
    try:
        r = {}
        cumple = True
        
        ev_ebitda = info.get("enterpriseToEbitda")
        if ev_ebitda:
            r["EV_EBITDA"] = round(ev_ebitda, 2)
            if ev_ebitda >= 5: cumple = False
        else:
            r["EV_EBITDA"] = None
        
        mc = info.get("marketCap")
        fcf = info.get("freeCashflow")
        if mc and fcf and fcf > 0:
            r["P_FCF"] = round(mc / fcf, 2)
            if r["P_FCF"] >= 8: cumple = False
        else:
            r["P_FCF"] = None
        
        pb = info.get("priceToBook")
        if pb:
            r["P_Book"] = round(pb, 2)
            if pb >= 1.5: cumple = False
        else:
            r["P_Book"] = None
        
        r["cumple_valoracion"] = cumple
        return r
    except Exception:
        return {"cumple_valoracion": False}


def valoracion_bancos(info):
    """Protocolo Bancos (pág 2): P/Book<1.0, P/E<10, ROE>12%"""
    try:
        r = {}
        cumple = True
        
        pb = info.get("priceToBook")
        if pb:
            r["P_Book"] = round(pb, 2)
            if pb >= 1.0: cumple = False
        else:
            r["P_Book"] = None
        
        pe = info.get("trailingPE", info.get("forwardPE"))
        if pe:
            r["PE"] = round(pe, 2)
            if pe >= 10: cumple = False
        else:
            r["PE"] = None
        
        roe = info.get("returnOnEquity")
        if roe:
            r["ROE"] = round(roe * 100, 2)
            if r["ROE"] <= 12: cumple = False
        else:
            r["ROE"] = None
        
        r["cumple_valoracion"] = cumple
        return r
    except Exception:
        return {"cumple_valoracion": False}


def aplicar_valoracion_sectorial(info):
    """Dispatcher: detecta sector y aplica protocolo correspondiente (pág 2)."""
    sector = obtener_sector_normalizado(info)
    dispatch = {
        "tecnologia": valoracion_tecnologia,
        "energia":    valoracion_energia,
        "bancos":     valoracion_bancos,
    }
    func = dispatch.get(sector, valoracion_industrial)
    return sector, func(info)


# ============================================================================
# PASO 4: FILTRO ANTI-CUCHILLO / TÉCNICO (pág 2 del PDF - CRÍTICO)
# ============================================================================
# Condiciones OBLIGATORIAS:
#   1. Precio actual > SMA(50)
#   2. RSI(14) > 40
# ============================================================================

def calcular_sma(precios, periodo=50):
    """Media Móvil Simple de N periodos."""
    if len(precios) < periodo:
        return None
    return precios[-periodo:].mean()


def calcular_rsi(precios, periodo=14):
    """RSI (Relative Strength Index) de N periodos."""
    if len(precios) < periodo + 1:
        return None
    
    deltas = precios.diff().dropna()
    ganancia = deltas.where(deltas > 0, 0)
    perdida = -deltas.where(deltas < 0, 0)
    
    avg_gain = ganancia.rolling(window=periodo, min_periods=periodo).mean()
    avg_loss = perdida.rolling(window=periodo, min_periods=periodo).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi.iloc[-1] if not rsi.empty else None


def filtro_tecnico_anti_cuchillo(ticker_str):
    """
    Filtro Anti-Cuchillo (pág 2 del PDF):
    - Precio > SMA(50)
    - RSI(14) > 40

    NOTA: precio_actual se obtiene SIEMPRE de los datos históricos descargados
    (NO de info['regularMarketPrice']) para garantizar coherencia de escala.
    Corrección automática de GBp -> GBP para acciones del LSE (.L).
    """
    try:
        hist = yf.download(ticker_str, period="6mo", interval="1d",
                          progress=False, auto_adjust=True)

        if hist.empty or len(hist) < 50:
            return {"precio": None, "sma50": None, "rsi14": None, "cumple_tecnico": False}

        precios_close = hist["Close"].squeeze()

        # Correción GBp -> GBP: Yahoo Finance devuelve acciones de la LSE (.L) en peniques.
        # Detectamos la inconsistencia comparando con regularMarketPrice de info.
        # Si el precio histórico es ~100x el precio de info, dividimos por 100.
        if ticker_str.upper().endswith('.L'):
            try:
                info_price = yf.Ticker(ticker_str).fast_info.get('lastPrice', None)
                hist_price_last = float(precios_close.iloc[-1])
                if info_price and info_price > 0:
                    ratio = hist_price_last / info_price
                    if ratio > 50:  # El hist está en peniques y el info en libras
                        precios_close = precios_close / 100
            except Exception:
                pass

        precio_actual = float(precios_close.iloc[-1])
        sma50 = calcular_sma(precios_close, 50)
        rsi14 = calcular_rsi(precios_close, 14)

        cumple = (
            (sma50 is not None and precio_actual > float(sma50)) and
            (rsi14 is not None and float(rsi14) > 40)
        )

        return {
            "precio": round(precio_actual, 2),
            "sma50": round(float(sma50), 2) if sma50 is not None else None,
            "rsi14": round(float(rsi14), 2) if rsi14 is not None else None,
            "cumple_tecnico": cumple
        }
    except Exception:
        return {"precio": None, "sma50": None, "rsi14": None, "cumple_tecnico": False}


# ============================================================================
# MOTOR PRINCIPAL: Lógica de Ejecución (pág 2 del PDF)
# ============================================================================
# Pipeline secuencial con manejo silencioso de errores:
#   1. Filtros de Calidad → descarta empresas débiles
#   2. Anti-Dilución → descarta diluidoras
#   3. Valoración Sectorial → filtros por sector
#   4. Técnico Anti-Cuchillo → confirma timing
#   5. Recomendación final
# ============================================================================

def analizar_ticker(ticker_str):
    """
    Analiza un ticker individual pasándolo por todos los filtros.
    Manejo de errores silencioso: retorna None si falla.
    """
    resultado = {"Ticker": ticker_str}

    try:
        stock = yf.Ticker(ticker_str)
        info = stock.info

        # Validación rápida: si no hay datos básicos, saltar
        if not info or not info.get("regularMarketPrice"):
            return None

        # --- FILTRO DE LIQUIDEZ MÍNIMA ---
        # Umbral conservador: 10.000 acciones/día (ejecutable en una ventana de 24h).
        # Permite small-caps y empresas poco conocidas pero descarta las completamente iliquidas.
        avg_vol = info.get("averageVolume", 0) or 0
        if avg_vol < 10_000:
            return None

        financials = stock.financials
        balance    = stock.balance_sheet
        cashflow   = stock.cashflow

        if financials.empty or balance.empty:
            return None
        # --- PASO 1: Filtros de Calidad (pág 1) ---
        calidad = filtros_calidad(info, financials, balance, cashflow)
        resultado["ROIC"] = calidad["roic"]
        resultado["Deuda_EBITDA"] = calidad["deuda_ebitda"]
        resultado["Interest_Coverage"] = calidad["interest_coverage"]
        resultado["Piotroski_Score"] = calidad["piotroski"]
        resultado["Cumple_Calidad"] = calidad["cumple_calidad"]
        
        # --- PASO 2: Anti-Dilución (pág 1-2) ---
        dilucion = filtro_anti_dilucion(balance)
        resultado["Dilucion_Anual_Pct"] = dilucion["dilucion_anual_pct"]
        resultado["Cumple_Dilucion"] = dilucion["cumple_dilucion"]
        
        # --- PASO 3: Valoración Sectorial (pág 2) ---
        sector_raw = info.get("sector", "Unknown")
        sector_norm, valoracion = aplicar_valoracion_sectorial(info)
        resultado["Sector"] = sector_raw
        resultado["Sector_Protocolo"] = sector_norm
        resultado["Cumple_Valoracion"] = valoracion.get("cumple_valoracion", False)
        resultado["Detalle_Valoracion"] = str({
            k: v for k, v in valoracion.items() if k != "cumple_valoracion"
        })
        
        # --- PASO 4: Filtro Técnico Anti-Cuchillo (pág 2 - CRÍTICO) ---
        tecnico = filtro_tecnico_anti_cuchillo(ticker_str)
        resultado["Precio"] = tecnico["precio"]
        resultado["SMA50"] = tecnico["sma50"]
        resultado["RSI14"] = tecnico["rsi14"]
        resultado["Cumple_Tecnico"] = tecnico["cumple_tecnico"]
        
        # --- PASO 5: Cumplimiento Fundamental Global ---
        resultado["Cumple_Fundamental"] = (
            calidad["cumple_calidad"] and
            dilucion["cumple_dilucion"] and
            valoracion.get("cumple_valoracion", False)
        )
        
        # --- RECOMENDACIÓN FINAL ---
        if resultado["Cumple_Fundamental"] and resultado["Cumple_Tecnico"]:
            resultado["Recomendacion"] = "COMPRA - Ganga Generacional"
        elif resultado["Cumple_Fundamental"] and not resultado["Cumple_Tecnico"]:
            resultado["Recomendacion"] = "WATCHLIST - Esperar senal tecnica"
        elif not resultado["Cumple_Fundamental"] and resultado["Cumple_Tecnico"]:
            resultado["Recomendacion"] = "REVISAR - Fundamentales debiles"
        else:
            resultado["Recomendacion"] = "DESCARTAR"
        
        return resultado
    
    except Exception:
        # Manejo de errores silencioso: no imprime nada, simplemente ignora
        return None


def ejecutar_criba(extra_tickers=None):
    """
    Función principal: ejecuta la criba masiva con barra de progreso.
    Anti-bloqueo con delay entre peticiones.
    """
    print()
    print("=" * 70)
    print("  CRIBA DE GANGAS GENERACIONALES v2.1 - Deep Value Screening")
    print(f"  Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    # --- Cargar tickers ---
    tickers, fuente = obtener_tickers(extra_tickers)
    print(f"  📋 Fuente: {fuente}")
    print(f"  📊 Total tickers: {len(tickers)}")
    print(f"  ⏱️  Delay anti-bloqueo: {DELAY_ENTRE_PETICIONES}s por ticker")
    
    tiempo_estimado = len(tickers) * (DELAY_ENTRE_PETICIONES + 2)  # ~2s por análisis
    minutos = int(tiempo_estimado // 60)
    print(f"  ⏳ Tiempo estimado: ~{minutos} minutos")
    print()
    
    resultados = []
    errores = 0
    
    # --- Barra de progreso con tqdm ---
    barra = tqdm(
        tickers,
        desc="  Analizando",
        unit="ticker",
        bar_format="  {l_bar}{bar:40}{r_bar}",
        colour="green",
        ncols=90
    )
    
    for ticker in barra:
        barra.set_postfix_str(f"{ticker:6s} | OK:{len(resultados)} ERR:{errores}")
        
        resultado = analizar_ticker(ticker)
        
        if resultado:
            resultados.append(resultado)
        else:
            errores += 1
        
        # Anti-bloqueo: pausa entre peticiones a Yahoo Finance
        time.sleep(DELAY_ENTRE_PETICIONES)
    
    print("\n" + "=" * 70)
    
    if not resultados:
        print("  ❌ No se obtuvieron resultados válidos.")
        return None
    
    # --- Crear DataFrame ordenado ---
    columnas_orden = [
        "Ticker", "Precio", "Sector", "Sector_Protocolo",
        "ROIC", "Piotroski_Score", "Deuda_EBITDA", "Interest_Coverage",
        "Dilucion_Anual_Pct", "SMA50", "RSI14",
        "Cumple_Calidad", "Cumple_Dilucion", "Cumple_Valoracion",
        "Cumple_Fundamental", "Cumple_Tecnico",
        "Recomendacion", "Detalle_Valoracion"
    ]
    
    df = pd.DataFrame(resultados)
    columnas_existentes = [c for c in columnas_orden if c in df.columns]
    df = df[columnas_existentes]
    
    # --- RESUMEN ---
    gangas = df[df["Recomendacion"].str.contains("COMPRA", na=False)]
    watchlist = df[df["Recomendacion"].str.contains("WATCHLIST", na=False)]
    revisar = df[df["Recomendacion"].str.contains("REVISAR", na=False)]
    descartar = df[df["Recomendacion"].str.contains("DESCARTAR", na=False)]
    
    print()
    print("  RESUMEN DE RESULTADOS")
    print("  " + "-" * 50)
    print(f"  Tickers procesados:        {len(tickers)}")
    print(f"  Datos validos obtenidos:    {len(df)}")
    print(f"  Tickers sin datos/error:    {errores}")
    print()
    print(f"  [COMPRA]    Gangas Generacionales:  {len(gangas)}")
    print(f"  [WATCHLIST] Esperar senal tecnica:   {len(watchlist)}")
    print(f"  [REVISAR]   Fundamentales debiles:   {len(revisar)}")
    print(f"  [DESCARTAR] No cumplen filtros:      {len(descartar)}")
    
    if not gangas.empty:
        print(f"\n  {'='*65}")
        print("  GANGAS GENERACIONALES DETECTADAS:")
        print(f"  {'='*65}")
        for _, row in gangas.iterrows():
            precio = f"${row['Precio']}" if row['Precio'] else "N/A"
            roic = f"{row.get('ROIC', 'N/A')}%" if row.get('ROIC') else "N/A"
            pio = row.get('Piotroski_Score', 'N/A')
            print(f"    * {row['Ticker']:6s} | {precio:>10} | {row['Sector']:22s} "
                  f"| ROIC: {roic:>8} | F-Score: {pio}")
    
    if not watchlist.empty:
        print(f"\n  {'='*65}")
        print("  WATCHLIST (esperando confirmacion tecnica):")
        print(f"  {'='*65}")
        for _, row in watchlist.iterrows():
            precio = f"${row['Precio']}" if row['Precio'] else "N/A"
            rsi = f"{row.get('RSI14', 'N/A')}" if row.get('RSI14') else "N/A"
            sma = f"${row.get('SMA50', 'N/A')}" if row.get('SMA50') else "N/A"
            print(f"    > {row['Ticker']:6s} | {precio:>10} | {row['Sector']:22s} "
                  f"| RSI: {rsi:>6} | SMA50: {sma}")
    
    # --- EXPORTAR CSV ---
    df.to_csv(CSV_SALIDA, index=False, encoding="utf-8-sig")
    print(f"\n  Resultados exportados a: {CSV_SALIDA}")
    print("=" * 70)
    
    return df


# ============================================================================
# EJECUCIÓN
# ============================================================================
if __name__ == "__main__":
    df_resultados = ejecutar_criba()