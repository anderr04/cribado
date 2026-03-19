import schedule
import time
import json
import os
import signal
import sys
from bot_logic import TradingBot

def handle_sigterm(signum, frame):
    print("\n[SISTEMA] Recibida senal de apagado (SIGTERM/SIGINT). Cerrando bot de forma segura...")
    sys.exit(0)

# Capturar senales de cierre para apagado seguro (tmux, nohup, kill)
signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

def main():
    config_path = "config.json"
    bot = TradingBot(config_path)
    
    print("\n[SISTEMA] Inicializando Paper Trading Bot...")
    
    # Ejecutar una iteracion al arrancar: si el CSV es reciente (< 23h) se saltara la criba.
    # Si no existe o es antiguo, lanzara la criba completa ahora.
    bot.run_iteration()
    
    # Cargar configuracion para el programador
    run_interval = bot.config.get("run_interval_days", 1)  # Por defecto cada 1 dia
    run_time = bot.config.get("run_time", "23:00")         # Por defecto a las 23:00
    
    # Programar la iteracion principal (FORZANDO criba independientemente de la edad del CSV local)
    schedule.every(run_interval).days.at(run_time).do(lambda: bot.run_iteration(force_screening=True))
    
    print(f"\n[SCHEDULER] Configurando ejecucion cada {run_interval} dia(s) a las {run_time}.")
    
    # Configurar horario (schedule)
    if run_interval == 1:
        schedule.every().day.at(run_time).do(bot.run_iteration)
    else:
        schedule.every(run_interval).days.at(run_time).do(bot.run_iteration)
        
    print("[SCHEDULER] Bot en ejecucion 24/7. Esperando siguientes trabajos...\n")
    
    # Bucle infinito 24/7
    while True:
        schedule.run_pending()
        time.sleep(60)  # Checkeo cada minuto

if __name__ == "__main__":
    main()
