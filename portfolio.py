import sqlite3
import datetime
import json
import os

class Portfolio:
    def __init__(self, config_path="config.json"):
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.config = json.load(f)
        else:
            self.config = {
                "initial_balance": 10000.0,
                "db_file": "portfolio.db"
            }

        self.db_path = self.config.get("db_file", "portfolio.db")
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Inicializa las tablas si no existen e inserta el balance inicial."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Tabla de estado de cartera (solo guarda el cash)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cash REAL NOT NULL
                )
            ''')

            # Tabla de posiciones actuales (incluye purchase_date para Stop-Loss y WATCHLIST timeout)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    ticker TEXT PRIMARY KEY,
                    shares REAL NOT NULL,
                    average_price REAL NOT NULL,
                    purchase_date TEXT NOT NULL DEFAULT '2000-01-01'
                )
            ''')

            # Migración silenciosa: si la columna purchase_date no existe en la DB existente, añadirla
            try:
                cursor.execute("ALTER TABLE positions ADD COLUMN purchase_date TEXT NOT NULL DEFAULT '2000-01-01'")
            except Exception:
                pass  # Ya existe, ignorar

            # Historial de transacciones
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    shares REAL NOT NULL,
                    price REAL NOT NULL,
                    total REAL NOT NULL
                )
            ''')

            # Insertar balance inicial si la tabla esta vacía
            cursor.execute("SELECT COUNT(*) FROM portfolio_state")
            if cursor.fetchone()[0] == 0:
                initial_cash = self.config.get("initial_balance", 10000.0)
                cursor.execute("INSERT INTO portfolio_state (id, cash) VALUES (1, ?)", (initial_cash,))

            conn.commit()

    def get_cash(self):
        """Devuelve el balance actual en efectivo."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT cash FROM portfolio_state WHERE id = 1")
            row = cursor.fetchone()
            return row[0] if row else 0.0

    def get_positions(self):
        """Devuelve diccionario {ticker: {'shares', 'average_price', 'purchase_date'}}."""
        positions = {}
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ticker, shares, average_price, purchase_date FROM positions")
            for row in cursor.fetchall():
                positions[row[0]] = {
                    "shares": row[1],
                    "average_price": row[2],
                    "purchase_date": row[3]
                }
        return positions

    def has_position(self, ticker):
        """Verifica si el ticker ya está en cartera."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM positions WHERE ticker = ?", (ticker,))
            return cursor.fetchone() is not None

    def buy(self, ticker, price, total_investment):
        """
        Ejecuta compra simulada:
        1. Descuenta cash
        2. Añade/actualiza posiciones
        3. Registra el trade
        """
        # Rechazar órdenes demasiado pequeñas (evita compras de $0 cuando no hay cash)
        if total_investment < 1.0:
            print(f"  [RECHAZADA] Importe de compra insuficiente (${total_investment:.2f}) para {ticker}. Sin cash.")
            return False

        cash = self.get_cash()
        if cash < total_investment:
            print(f"  [RECHAZADA] No hay suficiente cash (${cash:.2f}) para comprar ${total_investment:.2f} de {ticker}")
            return False

        shares = total_investment / price
        timestamp = datetime.datetime.now().isoformat()
        today = datetime.date.today().isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            # 1. Actualizar cash
            new_cash = cash - total_investment
            cursor.execute("UPDATE portfolio_state SET cash = ? WHERE id = 1", (new_cash,))

            # 2. Actualizar posición (o insertar nueva con purchase_date = hoy)
            cursor.execute("SELECT shares, average_price FROM positions WHERE ticker = ?", (ticker,))
            row = cursor.fetchone()
            if row:
                old_shares = row[0]
                old_avg_price = row[1]
                new_shares = old_shares + shares
                new_avg_price = ((old_shares * old_avg_price) + (shares * price)) / new_shares
                cursor.execute(
                    "UPDATE positions SET shares = ?, average_price = ? WHERE ticker = ?",
                    (new_shares, new_avg_price, ticker)
                )
            else:
                cursor.execute(
                    "INSERT INTO positions (ticker, shares, average_price, purchase_date) VALUES (?, ?, ?, ?)",
                    (ticker, shares, price, today)
                )

            # 3. Registrar trade
            cursor.execute('''
                INSERT INTO trades (timestamp, action, ticker, shares, price, total)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (timestamp, "BUY", ticker, shares, price, total_investment))

            conn.commit()

        print(f"  [COMPRA] {ticker}: {shares:.4f} shares a ${price:.2f} (Total: ${total_investment:.2f})")
        return True

    def sell(self, ticker, price, reason=""):
        """
        Ejecuta venta total de la posición simulada:
        1. Aumenta cash
        2. Elimina la posición
        3. Registra el trade
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT shares, average_price FROM positions WHERE ticker = ?", (ticker,))
            row = cursor.fetchone()

            if not row:
                print(f"  [INFO] No hay posicion de {ticker} en cartera para vender.")
                return False

            shares = row[0]
            avg_price = row[1]
            total_return = shares * price
            profit_loss = total_return - (shares * avg_price)
            profit_pct = (profit_loss / (shares * avg_price)) * 100 if avg_price > 0 else 0

            timestamp = datetime.datetime.now().isoformat()

            # 1. Actualizar cash
            cursor.execute("SELECT cash FROM portfolio_state WHERE id = 1")
            current_cash = cursor.fetchone()[0]
            new_cash = current_cash + total_return
            cursor.execute("UPDATE portfolio_state SET cash = ? WHERE id = 1", (new_cash,))

            # 2. Eliminar posición
            cursor.execute("DELETE FROM positions WHERE ticker = ?", (ticker,))

            # 3. Registrar trade
            cursor.execute('''
                INSERT INTO trades (timestamp, action, ticker, shares, price, total)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (timestamp, "SELL", ticker, shares, price, total_return))

            conn.commit()

        reason_str = f" [{reason}]" if reason else ""
        print(f"  [VENTA]{reason_str} {ticker}: {shares:.4f} shares a ${price:.2f} "
              f"(Total: ${total_return:.2f}) | PnL: ${profit_loss:.2f} ({profit_pct:.2f}%)")
        return True

    def get_portfolio_summary(self):
        """Devuelve resumen de cartera."""
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
