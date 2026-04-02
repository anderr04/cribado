# Bolsa Cribado - Live Trading Bot 📈

Este repositorio contiene un bot automatizado diseñado para escanear de forma exhaustiva el mercado (mediante un filtro *Deep Value Screening*) y gestionar de forma autónoma una cartera de posiciones accionaria.
Está diseñado para emular meticulosamente condiciones de operativa con dinero real (Spreads simulados, *Slippage*, Ajuste real de Stock Splits, Acciones Fraccionarias, Sizing de Capital, etc).

## ✨ Características Principales
- **Deep Value Screener:** Utiliza `yfinance` para descartar ruido y centrarse en "Gangas Generacionales". Utiliza métricas como ROIC, Piotroski F-Score y un filtro técnico con medias móviles para evitar "value traps".
- **Gestión Autónoma (24/7):** Opera mediante un `schedule` que desencadena rondas diarias a las 23:00.
- **Stop-Loss y Expiración:** Venta automática con un **-30%** real (ajustado por splits) y Timeout automático tras 45 días continuados sin fuerza relativa (Watchlist phase).
- **Gestión de Liquidez Rigurosa (Position Sizing):** Asignación estricta de un **4% de Total Portfolio Value** a cada señal generada.
- **Slippage Alpaca:** Penalización fija del 0.1% a favor del mercado en *bid* y *ask* para dotar de solidez institucional a los beneficios simulados.

## 📂 Estructura del Repositorio

- `bot_logic.py`: Motor de toma de decisiones, chequeo de Splits y bucles de Stop-Loss.
- `criba_empresas.py`: Screener fundamental que inspecciona la base de datos descargando perfiles a tiempo real.
- `portfolio.py`: El "bróker interno" o Ledger. Almacena transacciones y calcula participaciones, aplicando el slippage.
- `main.py`: Entrada del Bot y planificador de Timeframes 24/7.
- `analysis.py`: Ejecutable lateral para obtener métricas (PnL, Rendimientos, Posiciones abiertas) bajo demanda sin pausar el sistema.
- `data/`: Almacenamiento local aislado. Contiene el estado (`portfolio_state.json`), histórico (`trades_history.csv`) y los datasets del universo de exploración.

## 🚀 Despliegue en Servidor o Máquina Virtual

Para poner el bot en marcha dentro de un entorno aislado (ej. una VM en la nube) corriendo de forma perpetua:

### 1. Clonar e Instalar Dependencias
```bash
# Limpiar instalaciones heredadas (opcional)
rm -rf Bolsa_cribado
git clone <TU_REPOSITORIO> Bolsa_cribado
cd Bolsa_cribado

# Crear un entorno virtual estable
python3 -m venv venv
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Arrancar en Segundo Plano (Tmux)
Te recomendamos encarecidamente utilizar `tmux` puesto que este bot requiere una vida útil larga. Las desconexiones SSH no interrumpirán tu sesión.
```bash
tmux new -s bot_acciones
python main.py
```
*(Para salir de tmux y dejarlo corriendo en segundo plano pulsa `CTRL+B` seguido de la letra `D`)*.

### 3. Monitorear Resultados
Para leer tus resultados, utilidades o Win Rate simple con tu PnL, abre el terminal y haz:
```bash
source venv/bin/activate
python analysis.py
```
