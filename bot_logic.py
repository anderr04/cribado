import os
import gc
import json
import time
import pandas as pd
from datetime import datetime, date
from portfolio import Portfolio
from criba_empresas import ejecutar_criba, CSV_SALIDA

# ============================================================
# PARÁMETROS DE GESTIÓN DE RIESGO
# ============================================================
STOP_LOSS_PCT       = -30.0   # Venta automática si la posición cae > 30% desde precio de compra
MAX_WATCHLIST_DIAS  = 45      # Venta si lleva > 45 días en cartera con estado WATCHLIST
BUFFER_POSICIONES   = 5       # Reserva de liquidez (position_size = cash / (gangas + buffer))


class TradingBot:
    def __init__(self, config_path="config.json"):
        self.portfolio = Portfolio(config_path)
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.config = json.load(f)
        else:
            self.config = {}

    def _csv_es_reciente(self, max_horas=23):
        """Devuelve True si el CSV de salida existe y tiene menos de max_horas."""
        if not os.path.exists(CSV_SALIDA):
            return False
        edad_seg = time.time() - os.path.getmtime(CSV_SALIDA)
        return edad_seg < (max_horas * 3600)

    def _dias_en_cartera(self, purchase_date_str):
        """Calcula cuántos días han pasado desde la fecha de compra."""
        try:
            purchase = date.fromisoformat(purchase_date_str)
            return (date.today() - purchase).days
        except Exception:
            return 0

    def run_iteration(self, force_screening=False):
        """Bloque maestro que se ejecuta en cada intervalo planificado."""
        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INICIANDO FLUJO DEL BOT DE TRADING")
        print(f"{'='*60}")

        # 1. Tickers en cartera → se inyectan en la criba para asegurar seguimiento
        current_positions = list(self.portfolio.get_positions().keys())

        # 2. Ejecutar criba o cargar CSV reciente
        if not force_screening and self._csv_es_reciente():
            print(f">> CSV reciente encontrado ({CSV_SALIDA}). Saltando criba y cargando desde disco.")
            try:
                df_resultados = pd.read_csv(CSV_SALIDA)
            except Exception as e:
                print(f">> [ERROR] No se pudo leer el CSV: {e}")
                return
        else:
            print(">> Paso 1: Iniciando screening profundo (puede tardar horas)...")
            print(f">> Nota: Inyectando {len(current_positions)} posiciones de cartera para seguimiento.")
            df_resultados = ejecutar_criba(extra_tickers=current_positions)

            if df_resultados is None or df_resultados.empty:
                if os.path.exists(CSV_SALIDA):
                    print(">> Criba falló. Cargando CSV anterior como fallback...")
                    df_resultados = pd.read_csv(CSV_SALIDA)
                else:
                    print(">> [ERROR] Sin datos de screening. Saltando iteración.")
                    return

        print(f"\n>> Paso 2: Interpretando {len(df_resultados)} filas para Paper Trading...")

        # Mapa de precios actuales {ticker -> precio} desde el CSV fresco
        precio_mercado = (
            df_resultados.dropna(subset=['Precio'])
            .set_index('Ticker')['Precio']
            .to_dict()
        )

        # Mapa de recomendaciones {ticker -> recomendacion} para consulta rápida
        recomendacion_map = (
            df_resultados.dropna(subset=['Recomendacion'])
            .set_index('Ticker')['Recomendacion']
            .to_dict()
        )

        # Gangas detectadas hoy para el Position Sizing
        num_gangas = (df_resultados['Recomendacion'] == 'COMPRA - Ganga Generacional').sum()

        # Valor REAL de la cartera usando precios de mercado (no precios de compra)
        posiciones_actuales = self.portfolio.get_positions()
        cash_actual = self.portfolio.get_cash()
        valor_posiciones_mercado = sum(
            pos['shares'] * precio_mercado.get(ticker, pos['average_price'])
            for ticker, pos in posiciones_actuales.items()
        )
        total_portfolio_value_real = cash_actual + valor_posiciones_mercado

        # Position Sizing uniforme, calculado UNA VEZ antes del bucle
        amount_to_invest = cash_actual / (num_gangas + BUFFER_POSICIONES) if num_gangas > 0 else 0

        print(f"  Valor cartera (precio mercado): ${total_portfolio_value_real:,.2f}")
        print(f"  Cash disponible:                ${cash_actual:,.2f}")
        print(f"  Gangas detectadas hoy: {num_gangas} | Posicion uniforme: ${amount_to_invest:,.2f} (buffer={BUFFER_POSICIONES})")

        # ============================================================
        # PASO A: Gestión de riesgo sobre posiciones existentes
        # (Stop-Loss, WATCHLIST timeout) — ANTES del bucle de compras
        # ============================================================
        posiciones_actuales = self.portfolio.get_positions()
        for ticker, pos in posiciones_actuales.items():
            precio_actual = precio_mercado.get(ticker)
            if precio_actual is None:
                continue

            avg_price   = pos['average_price']
            pct_cambio  = ((precio_actual - avg_price) / avg_price * 100) if avg_price > 0 else 0
            dias_en_cartera = self._dias_en_cartera(pos.get('purchase_date', '2000-01-01'))
            recomendacion = recomendacion_map.get(ticker, 'DESCARTAR')

            # --- Stop-Loss: vender si la pérdida supera el umbral ---
            if pct_cambio <= STOP_LOSS_PCT:
                print(f"  [STOP-LOSS] {ticker} ha caido {pct_cambio:.1f}% desde compra (limite: {STOP_LOSS_PCT}%). Vendiendo.")
                self.portfolio.sell(ticker, float(precio_actual), reason="STOP-LOSS")
                continue

            # --- WATCHLIST timeout: vender si lleva demasiado tiempo sin señal técnica ---
            if recomendacion == "WATCHLIST - Esperar senal tecnica" and dias_en_cartera > MAX_WATCHLIST_DIAS:
                print(f"  [WATCHLIST-TIMEOUT] {ticker} lleva {dias_en_cartera} dias en cartera con senal WATCHLIST. Vendiendo.")
                self.portfolio.sell(ticker, float(precio_actual), reason="WATCHLIST-TIMEOUT")

        # ============================================================
        # PASO B: Bucle principal sobre el CSV
        # (Ventas por deterioro fundamental + Compras nuevas)
        # ============================================================
        for _, row in df_resultados.iterrows():
            ticker       = str(row['Ticker']).strip()
            recomendacion = str(row.get('Recomendacion', ''))
            precio_actual = row.get('Precio')

            if pd.isna(precio_actual):
                continue

            precio_float = float(precio_actual)

            # Releer posiciones después de cada operación para evitar dobles ventas
            posiciones_actuales = self.portfolio.get_positions()
            in_portfolio        = ticker in posiciones_actuales

            # --- VENTA por deterioro fundamental ---
            if in_portfolio and recomendacion not in ["COMPRA - Ganga Generacional", "WATCHLIST - Esperar senal tecnica"]:
                print(f"  [ALERTA] {ticker} cambio a '{recomendacion}'. Evaluando VENTA.")
                self.portfolio.sell(ticker, precio_float, reason="FUNDAMENTALES")

            # --- COMPRA ---
            elif not in_portfolio and recomendacion == "COMPRA - Ganga Generacional":
                print(f"  [ALERTA] {ticker} es Ganga Generacional. Evaluando COMPRA.")
                self.portfolio.buy(ticker, precio_float, amount_to_invest)

        # Resumen Final
        summary = self.portfolio.get_portfolio_summary()
        print(f"\n--- FIN DE ITERACION ---")
        print(f"  Cash Disponible:  ${summary['cash']:,.2f}")
        print(f"  Valor Invertido:  ${summary['invested']:,.2f}")
        print(f"  Valor Total Est:  ${summary['total_estimated']:,.2f}")
        print(f"  Posiciones Abiertas: {summary['num_positions']}")
        print(f"{'='*60}\n")

        # Liberar RAM para evitar OOM en VMs con poca memoria
        df_resultados = None
        gc.collect()
