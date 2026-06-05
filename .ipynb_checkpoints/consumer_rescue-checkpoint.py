import json
import os
import time
from collections import deque
from confluent_kafka import Consumer, KafkaError

# --- KONFIGURACJA ---
KAFKA_CONFIG = {
    'bootstrap.servers': 'broker:9092',
    'group.id': 'safety-automation-scada-group',
    'auto.offset.reset': 'latest'
}
TOPIC_NAME = 'nuclear-reactor-data'
CONTROL_FILE = "control.json"

consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([TOPIC_NAME])

history_size = 5
history = deque(maxlen=history_size)

current_active_safety = "NONE"
startup_messages_absorbed = 0
WARMUP_THRESHOLD = 15

print("=====================================================================")
print("=             ZAAWANSOWANY AUTOMATYCZNY SYSTEM RATUNKOWY            =")
print("=====================================================================")
print(f"Nasłuchiwanie na strumieniu: {TOPIC_NAME}")
print("Trwa kalibracja i synchronizacja czujników...\n")

def send_control_command(safety_action, reason):
    global current_active_safety
    if current_active_safety == safety_action:
        return
    
    command = {"safety_system": safety_action}
    try:
        with open(CONTROL_FILE, "w") as f:
            json.dump(command, f)
        
        current_active_safety = safety_action
        
        print("\n" + "!" * 90)
        print(f"[SCADA AUTOMATYKA] WYZWOLENIE REAKCJI RATUNKOWEJ -> {safety_action}")
        print(f"[POWÓD] {reason}")
        print(f"[TIMESTAMP] {time.strftime('%H:%M:%S')}")
        print("!" * 90 + "\n")
    except Exception as e:
        pass

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                break
            
        data = json.loads(msg.value().decode('utf-8'))
        
        sim_time = data['metadata']['timestamp']
        flux = data['core']['neutron_flux_pct']
        press = data['primary_loop']['pressure_mpa']
        flow = data['primary_loop']['flow_pct']
        temp_hot = data['primary_loop']['temp_hot_c']
        rad = data['safety']['radiation_usvh']
        
        history.append((sim_time, press, temp_hot))
        startup_messages_absorbed += 1

        if startup_messages_absorbed <= WARMUP_THRESHOLD:
            remaining = WARMUP_THRESHOLD - startup_messages_absorbed
            print(f"[WARM-UP] Synchronizacja bufora... Pozostało: {remaining}s | P: {press:.2f} MPa", end='\r')
            continue

        dp_dt = 0.0
        dt_dt = 0.0
        
        if len(history) == history_size:
            t_old, p_old, th_old = history[0]
            dt = sim_time - t_old
            if dt >= 2.0: 
                dp_dt = (press - p_old) / dt
                dt_dt = (temp_hot - th_old) / dt

        # ==========================================
        # LOGIKA MONITORINGU I SPRZĘŻENIA ZWROTNEGO 
        # ==========================================
        
        # OSTATECZNOŚĆ: SCRAM (Awaria pompy, całkowity blackout)
        if flow < 40.0 or temp_hot > 340.0:
            reason = f"KRYTYCZNY BRAK CHŁODZENIA! Temp: {temp_hot:.1f}°C, Przepływ: {flow:.1f}% -> Zrzut prętów!"
            send_control_command("SCRAM", reason)
            
        # OBRONA PRZED ROZERWANIEM: STEAM_DUMP (Nagły skok ciśnienia np. Turbine Trip)
        # Warunek: Promieniowanie musi być w normie, inaczej wypuścimy skażenie!
        elif (press > 15.5 or dp_dt > 0.06) and rad < 0.13:
            reason = f"Ekstremalny skok ciśnienia układu! Bezpieczny zrzut czystej pary. Ciśnienie: {press:.2f} MPa"
            send_control_command("STEAM_DUMP", reason)
            
        # OBRONA PRZED WYCIEKIEM (LEAK): EMERGENCY_BORON (Spadek ciśnienia + wzrost promieniowania)
        # Używamy boru, by powoli wygasić rdzeń bez zrzutu radioaktywnej pary na zewnątrz
        elif (dp_dt <= -0.02 and rad >= 0.13) or press < 13.0:
            reason = f"Wykryto dekompresję (LEAK)! Wtrysk boru dusi reakcję bez ryzyka skażenia środowiska zrzutem pary."
            send_control_command("EMERGENCY_BORON", reason)

        # AUTO-STABILIZACJA (Inteligentny powrót do normy)
        else:
            if current_active_safety == "STEAM_DUMP":
                # Wyłącz zrzut pary, gdy ciśnienie spadnie do bezpiecznego poziomu
                if press <= 14.8:
                    reason = f"Ciśnienie bezpieczne ({press:.2f} MPa). Zamykanie zaworów pary."
                    send_control_command("NONE", reason)
                    
            elif current_active_safety == "EMERGENCY_BORON":
                # Wyłącz awaryjne borowanie, gdy moc reaktora spadnie poniżej 20% i przestanie wytwarzać nadmiarowe ciepło
                if flux <= 20.0:
                    reason = f"Reakcja łańcuchowa ustabilizowana (Moc: {flux:.1f}%). Zakończenie wtrysku boru."
                    send_control_command("NONE", reason)

        if current_active_safety == "NONE":
            print(f"[{time.strftime('%H:%M:%S')}] SCADA State: MONITORING | P: {press:.2f} ({dp_dt:+.3f}) | T: {temp_hot:.1f} ({dt_dt:+.2f}) | Rad: {rad:.4f} uSv/h", end='\r')

except KeyboardInterrupt:
    print("\n[SCADA] Wyłączanie systemu automatyki...")
finally:
    consumer.close()
