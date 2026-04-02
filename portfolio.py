import json
import os
import csv
import datetime

class Portfolio:
    def __init__(self, config_path="config.json"):
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.config = json.load(f)
        else:
            self.config = {
                "initial_balance": 10000.0,
                "data_dir": "data"
            }

        self.data_dir = self.config.get("data_dir", "data")
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.state_file = os.path.join(self.data_dir, "portfolio_state.json")
        self.trades_file = os.path.join(self.data_dir, "trades_history.csv")
        self.slippage_pct = 0.001 # 0.1% Alpaca-like slippage

        self._init_data()

    def _init_data(self):
        # Initialize state json
        if not os.path.exists(self.state_file):
            initial_state = {
                "cash": self.config.get("initial_balance", 10000.0),
                "positions": {}
            }
            with open(self.state_file, "w") as f:
                json.dump(initial_state, f, indent=4)
                
        # Initialize trades csv
        if not os.path.exists(self.trades_file):
            with open(self.trades_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp", "Action", "Ticker", "Shares", "ExecutionPrice", "Total", "PnL", "PnL_Pct", "Reason"])

    def _load_state(self):
        with open(self.state_file, "r") as f:
            return json.load(f)
            
    def _save_state(self, state):
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=4)

    def get_cash(self):
        return self._load_state().get("cash", 0.0)

    def get_positions(self):
        return self._load_state().get("positions", {})

    def has_position(self, ticker):
        return ticker in self.get_positions()

    def adjust_for_split(self, ticker, split_ratio):
        """Ajusta una posición tras un stock split (split_ratio > 0)."""
        state = self._load_state()
        pos = state["positions"].get(ticker)
        if pos and split_ratio > 0 and split_ratio != 1:
            old_shares = pos["shares"]
            old_avg = pos["average_price"]
            
            # Si el split_ratio es p. ej. 2 (2 for 1)
            new_shares = old_shares * split_ratio
            new_avg = old_avg / split_ratio
            
            state["positions"][ticker]["shares"] = new_shares
            state["positions"][ticker]["average_price"] = new_avg
            
            self._save_state(state)
            print(f"  [SPLIT] Ajustada posicion de {ticker} por split ratio {split_ratio}. "
                  f"Shares: {old_shares:.4f} -> {new_shares:.4f}, AvgPrice: ${old_avg:.2f} -> ${new_avg:.2f}")
            return True
        return False

    def buy(self, ticker, price, total_investment):
        if total_investment < 1.0:
            print(f"  [RECHAZADA] Importe de compra insuficiente (${total_investment:.2f}) para {ticker}. Sin cash.")
            return False

        state = self._load_state()
        cash = state.get("cash", 0.0)
        
        if cash < total_investment:
            print(f"  [RECHAZADA] No hay suficiente cash (${cash:.2f}) para comprar ${total_investment:.2f} de {ticker}")
            return False

        # Apply slippage (buy at a higher price simulating ask)
        exec_price = price * (1 + self.slippage_pct)
        shares = total_investment / exec_price
        
        timestamp = datetime.datetime.now().isoformat()
        today = datetime.date.today().isoformat()

        # Update cash
        state["cash"] = cash - total_investment
        
        # Update or add position
        pos = state["positions"].get(ticker)
        if pos:
            old_shares = pos["shares"]
            old_avg = pos["average_price"]
            new_shares = old_shares + shares
            new_avg = ((old_shares * old_avg) + (shares * exec_price)) / new_shares
            
            state["positions"][ticker] = {
                "shares": new_shares,
                "average_price": new_avg,
                "purchase_date": pos.get("purchase_date", today)
            }
        else:
            state["positions"][ticker] = {
                "shares": shares,
                "average_price": exec_price,
                "purchase_date": today
            }
            
        self._save_state(state)
        
        # Record trade
        with open(self.trades_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, "BUY", ticker, shares, exec_price, total_investment, 0.0, 0.0, ""])

        print(f"  [COMPRA] {ticker}: {shares:.4f} shares a ${exec_price:.2f} (Total: ${total_investment:.2f})")
        return True

    def sell(self, ticker, price, reason=""):
        state = self._load_state()
        pos = state["positions"].get(ticker)
        
        if not pos:
            print(f"  [INFO] No hay posicion de {ticker} en cartera para vender.")
            return False
            
        shares = pos["shares"]
        avg_price = pos["average_price"]
        
        # Apply slippage (sell at a lower price simulating bid)
        exec_price = price * (1 - self.slippage_pct)
        
        total_return = shares * exec_price
        profit_loss = total_return - (shares * avg_price)
        profit_pct = (profit_loss / (shares * avg_price)) * 100 if avg_price > 0 else 0
        
        timestamp = datetime.datetime.now().isoformat()
        
        # Update cash and remove position
        state["cash"] += total_return
        del state["positions"][ticker]
        self._save_state(state)
        
        # Record trade
        with open(self.trades_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, "SELL", ticker, shares, exec_price, total_return, profit_loss, profit_pct, reason])

        reason_str = f" [{reason}]" if reason else ""
        print(f"  [VENTA]{reason_str} {ticker}: {shares:.4f} shares a ${exec_price:.2f} "
              f"(Total: ${total_return:.2f}) | PnL: ${profit_loss:.2f} ({profit_pct:.2f}%)")
        return True

    def get_portfolio_summary(self):
        cash = self.get_cash()
        positions = self.get_positions()
        
        invested_value = sum(pos["shares"] * pos["average_price"] for pos in positions.values())
        total_estimated = cash + invested_value
        
        return {
            "cash": cash,
            "invested": invested_value,
            "total_estimated": total_estimated,
            "num_positions": len(positions),
            "positions": positions
        }
