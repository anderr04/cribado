import json
import csv
import os
import pandas as pd
from datetime import datetime

class PortfolioAnalyzer:
    def __init__(self, data_dir="data"):
        self.state_file = os.path.join(data_dir, "portfolio_state.json")
        self.trades_file = os.path.join(data_dir, "trades_history.csv")

    def print_analysis(self):
        print(f"\n========================================================")
        print(f"   LIVE TRADING PORTFOLIO ANALYSIS")
        print(f"========================================================")

        if not os.path.exists(self.state_file) or not os.path.exists(self.trades_file):
            print("No data found. Ensure the bot has run at least once.")
            return

        # 1. State Analysis (Open Positions & Cash)
        with open(self.state_file, "r") as f:
            state = json.load(f)

        cash = state.get("cash", 0.0)
        positions = state.get("positions", {})
        
        invested_value = sum(pos["shares"] * pos["average_price"] for pos in positions.values())
        total_estimated = cash + invested_value

        print(f"\n--- PORTFOLIO STATE ---")
        print(f"Cash Available : ${cash:,.2f}")
        print(f"Invested Value : ${invested_value:,.2f}")
        print(f"Total Equity   : ${total_estimated:,.2f}")
        print(f"Open Positions : {len(positions)}")

        if positions:
            print(f"\n--- OPEN POSITIONS ---")
            for ticker, data in positions.items():
                print(f"  {ticker.ljust(6)} | {data['shares']:>8.4f} shares | Avg Price: ${data['average_price']:>7.2f} | PxDate: {data.get('purchase_date', '')}")

        # 2. Trades Analysis (Historical Performance)
        try:
            df = pd.read_csv(self.trades_file)
        except Exception as e:
            print(f"Error reading trades file: {e}")
            return
            
        if df.empty:
            print("\nNo historical trades yet.")
            return

        sells = df[df['Action'] == 'SELL'].copy()
        buys = df[df['Action'] == 'BUY']
        
        print(f"\n--- HISTORICAL TRADES ---")
        print(f"Total Trades   : {len(df)}")
        print(f"Total Buys     : {len(buys)}")
        print(f"Total Sells    : {len(sells)}")

        if not sells.empty:
            # PnL Metrics
            sells['PnL'] = pd.to_numeric(sells['PnL'], errors='coerce').fillna(0)
            total_pnl = sells['PnL'].sum()
            winning_trades = len(sells[sells['PnL'] > 0])
            losing_trades = len(sells[sells['PnL'] <= 0])
            win_rate = winning_trades / len(sells) * 100 if len(sells) > 0 else 0

            print(f"\n--- PERFORMANCE METRICS ---")
            print(f"Realized PnL   : ${total_pnl:+,.2f}")
            print(f"Win Rate       : {win_rate:.1f}% ({winning_trades} Win / {losing_trades} Loss)")
            
            best_trade = sells.loc[sells['PnL'].idxmax()]
            worst_trade = sells.loc[sells['PnL'].idxmin()]
            
            print(f"Best Trade     : {best_trade['Ticker']} (${best_trade['PnL']:+,.2f}) [{best_trade['Reason']}]")
            print(f"Worst Trade    : {worst_trade['Ticker']} (${worst_trade['PnL']:+,.2f}) [{worst_trade['Reason']}]")
            
        print(f"========================================================\n")

if __name__ == "__main__":
    analyzer = PortfolioAnalyzer()
    analyzer.print_analysis()
