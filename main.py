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

# Manejar senales para detener Docker con gracia
signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

def main():
    config_path = "config.json"
    bot = TradingBot(config_path)
    
    print("\n[SISTEMA] Inicializando Paper Trading Bot...")
    
    # Ejecutar una vez al arrancar el contenedor
    bot.run_iteration()
    
    # Cargar configuracion para el programador
    run_interval = 1
    run_time = "16:00"
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                cfg = json.load(f)
                run_interval = cfg.get("run_interval_days", 1)
                run_time = cfg.get("run_time", "16:00")
        except Exception as e:
            print(f"[ERROR] No se pudo leer config.json completo: {e}")
            
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
