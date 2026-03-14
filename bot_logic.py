import os
import json
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

    def run_iteration(self):
        """Bloque maestro que se ejecuta en cada intervalo."""
        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INICIANDO FLUJO DEL BOT DE TRADING")
        print(f"{'='*60}")
        
        # 1. Obtenemos las posiciones actuales para garantizar que se formen parte de la criba
        current_positions = list(self.portfolio.get_positions().keys())
        
        # 2. Ejecutar la criba masiva que genera/actualiza el CSV de salida (tarda horas)
        print(">> Paso 1: Iniciando screening profundo...")
        print(f">> Nota: Se inyectan {len(current_positions)} posiciones de cartera para seguimiento.")
        
        # EJECUCIÓN (Generará gangas_generacionales_v2.csv)
        df_resultados = ejecutar_criba(extra_tickers=current_positions)
        
        # Como red de seguridad, en caso de fallar o devolver None, intentamos cargar desde el disco
        if df_resultados is None or df_resultados.empty:
            if os.path.exists(CSV_SALIDA):
                print(f">> Criba falló u omitida. Cargando resultados del archivo '{CSV_SALIDA}'...")
                df_resultados = pd.read_csv(CSV_SALIDA)
            else:
                print(">> [ERROR] No hay datos de screening para continuar. Saltando esta iteración.")
                return

        print(f"\n>> Paso 2: Interpretando resultados desde CSV ({len(df_resultados)} filas) para Paper Trading...")
        current_portfolio_dict = self.portfolio.get_positions()
        
        for index, row in df_resultados.iterrows():
            ticker = str(row['Ticker']).strip()
            recomendacion = str(row.get('Recomendacion', ''))
            precio_actual = row.get('Precio')
            
            if pd.isna(precio_actual):
                continue
            
            in_portfolio = ticker in current_portfolio_dict
            
            # --- Lógica de VENTA ---
            # Vendemos si ya lo tenemos y la recomendación empeora (deja de ser COMPRA o WATCHLIST)
            if in_portfolio and recomendacion not in ["COMPRA - Ganga Generacional", "WATCHLIST - Esperar senal tecnica"]:
                print(f"  [ALERTA] {ticker} cambió a '{recomendacion}'. Evaluando VENTA.")
                self.portfolio.sell(ticker, float(precio_actual))
            
            # --- Lógica de COMPRA ---
            # Compramos si no lo tenemos y la criba dicta COMPRA
            elif not in_portfolio and recomendacion == "COMPRA - Ganga Generacional":
                total_portfolio_value = self.portfolio.get_portfolio_summary()["total_estimated"]
                pct = self.config.get("position_size_pct", 0.05)
                amount_to_invest = total_portfolio_value * pct
                
                print(f"  [ALERTA] {ticker} cumple criterios de Ganga Generacional. Evaluando COMPRA.")
                self.portfolio.buy(ticker, float(precio_actual), amount_to_invest)
                
        # Imprimir Resumen Final
        summary = self.portfolio.get_portfolio_summary()
        print(f"\n--- FIN DE ITERACION DE TRADING ---")
        print(f"  Cash Disponible: ${summary['cash']:,.2f}")
        print(f"  Valor Invertido: ${summary['invested']:,.2f}")
        print(f"  Valor Total Est: ${summary['total_estimated']:,.2f}")
        print(f"  Posiciones Abiertas: {summary['num_positions']}")
        print(f"{'='*60}\n")

