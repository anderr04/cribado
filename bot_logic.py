import os
import json
import gc
import time
import pandas as pd
from datetime import datetime
from portfolio import Portfolio
from criba_empresas import ejecutar_criba, CSV_SALIDA

class TradingBot:
    def __init__(self, config_path="config.json"):
        self.portfolio = Portfolio(config_path)
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.config = json.load(f)
        else:
            self.config = {}

    def _csv_es_reciente(self, max_horas=23):
        """Comprueba si el CSV de salida existe y fue generado hace menos de max_horas."""
        if not os.path.exists(CSV_SALIDA):
            return False
        edad_segundos = time.time() - os.path.getmtime(CSV_SALIDA)
        return edad_segundos < (max_horas * 3600)

    def run_iteration(self):
        """Bloque maestro que se ejecuta en cada intervalo."""
        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INICIANDO FLUJO DEL BOT DE TRADING")
        print(f"{'='*60}")
        
        # 1. Obtenemos las posiciones actuales para garantizar que entren en la criba
        current_positions = list(self.portfolio.get_positions().keys())
        
        # 2. Ejecutar la criba o cargar desde CSV si ya es reciente (evita escaneos innecesarios al reiniciar)
        if self._csv_es_reciente():
            print(f">> CSV de salida reciente encontrado ({CSV_SALIDA}). Saltando criba y cargando desde disco.")
            try:
                df_resultados = pd.read_csv(CSV_SALIDA)
            except Exception as e:
                print(f">> [ERROR] No se pudo leer el CSV: {e}")
                return
        else:
            print(">> Paso 1: Iniciando screening profundo (esto puede tardar horas)...")
            print(f">> Nota: Se inyectan {len(current_positions)} posiciones de cartera para seguimiento.")
            df_resultados = ejecutar_criba(extra_tickers=current_positions)

            if df_resultados is None or df_resultados.empty:
                if os.path.exists(CSV_SALIDA):
                    print(f">> Criba fallo. Cargando CSV anterior como fallback...")
                    df_resultados = pd.read_csv(CSV_SALIDA)
                else:
                    print(">> [ERROR] No hay datos de screening para continuar. Saltando iteración.")
                    return

        print(f"\n>> Paso 2: Interpretando {len(df_resultados)} filas para Paper Trading...")

        # Construir un mapa de precios actuales desde el CSV para valoraciones correctas
        # { ticker -> precio_actual } usando los precios frescos del screening
        precio_mercado = (
            df_resultados.dropna(subset=['Precio'])
            .set_index('Ticker')['Precio']
            .to_dict()
        )

        # Calcular el total de gangas detectadas para el Position Sizing dinámico
        gangas_df = df_resultados[df_resultados['Recomendacion'] == 'COMPRA - Ganga Generacional']
        num_gangas = len(gangas_df)

        # Valor REAL de la cartera usando precios de mercado actuales (no precios de compra promedio)
        posiciones_actuales = self.portfolio.get_positions()
        cash_actual = self.portfolio.get_cash()
        valor_posiciones_mercado = sum(
            pos['shares'] * precio_mercado.get(ticker, pos['average_price'])
            for ticker, pos in posiciones_actuales.items()
        )
        total_portfolio_value_real = cash_actual + valor_posiciones_mercado

        print(f"  Valor en cartera (mercado actual): ${total_portfolio_value_real:,.2f}")
        print(f"  Cash disponible: ${cash_actual:,.2f}")
        print(f"  Gangas detectadas hoy: {num_gangas} | Tamaño de posición: ${cash_actual / (num_gangas + 10):,.2f}")

        for index, row in df_resultados.iterrows():
            ticker = str(row['Ticker']).strip()
            recomendacion = str(row.get('Recomendacion', ''))
            precio_actual = row.get('Precio')

            if pd.isna(precio_actual):
                continue

            precio_float = float(precio_actual)

            # Releer posiciones actualizadas en cada vuelta para reflejar ventas previas
            posiciones_actuales = self.portfolio.get_positions()
            in_portfolio = ticker in posiciones_actuales

            # --- Lógica de VENTA ---
            # Vendemos si lo tenemos y la recomendación empeora (deja de ser COMPRA o WATCHLIST)
            if in_portfolio and recomendacion not in ["COMPRA - Ganga Generacional", "WATCHLIST - Esperar senal tecnica"]:
                print(f"  [ALERTA] {ticker} cambiado a '{recomendacion}'. Evaluando VENTA.")
                self.portfolio.sell(ticker, precio_float)

            # --- Lógica de COMPRA ---
            # Compramos si no lo tenemos y la criba dicta COMPRA
            elif not in_portfolio and recomendacion == "COMPRA - Ganga Generacional":
                # Sizing Dinámico: cash_real / (gangas_de_hoy + 10 de buffer)
                # Usamos solo el cash disponible, NO el valor no realizado de las posiciones
                cash_actual = self.portfolio.get_cash()
                amount_to_invest = cash_actual / (num_gangas + 10)

                print(f"  [ALERTA] {ticker} es Ganga Generacional. Evaluando COMPRA.")
                self.portfolio.buy(ticker, precio_float, amount_to_invest)

        # Resumen Final
        summary = self.portfolio.get_portfolio_summary()
        print(f"\n--- FIN DE ITERACION DE TRADING ---")
        print(f"  Cash Disponible:  ${summary['cash']:,.2f}")
        print(f"  Valor Invertido:  ${summary['invested']:,.2f}")
        print(f"  Valor Total Est:  ${summary['total_estimated']:,.2f}")
        print(f"  Posiciones Abiertas: {summary['num_positions']}")
        print(f"{'='*60}\n")

        # Liberar RAM explícitamente para evitar OOM en VMs con poca memoria
        df_resultados = None
        gc.collect()
