import json
import time
from collections import deque
from confluent_kafka import Consumer, KafkaError

# --- KONFIGURACJA ---
KAFKA_CONFIG = {
    'bootstrap.servers': 'broker:9092',
    'group.id': 'trend-analyzer-group',
    'auto.offset.reset': 'latest'
}
TOPIC_NAME = 'nuclear-reactor-data'

consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([TOPIC_NAME])

# Pamięć podręczna na odczyty z ostatnich 5 sekund
history_size = 5
press_history = deque(maxlen=history_size)
temp_history = deque(maxlen=history_size)

# --- DEFINICJA PROGÓW BEZPIECZEŃSTWA (TWARDE LIMITY) ---
CRITICAL_PRESS_LOW = 13.0   # Dolny próg ciśnienia (wyciek)
CRITICAL_PRESS_HIGH = 17.0  # Górny próg ciśnienia (przeładowanie)
CRITICAL_TEMP_HIGH = 320.0  # Górny próg temperatury (przegrzanie rdzenia)

# --- PUNKTY STABILIZACJI I MARGINESY (STREFY WYCISZENIA) ---
TARGET_PRESS = 15.07        # Docelowe ciśnienie (MPa)
TARGET_TEMP = 305.40        # Docelowa temperatura (°C)

PRESS_STABLE_ZONE = 0.05    # Margines tolerancji ciśnienia (±0.05 MPa)
TEMP_STABLE_ZONE = 0.5      # Margines tolerancji temperatury (±0.5 °C)

# --- FLAGI JEDNORAZOWEGO WYŚWIETLENIA WEJŚCIA W STREFĘ STABILIZACJI ---
press_inside_stable_zone = False
temp_inside_stable_zone = False

# --- KONTROLA CZASU WYŚWIETLANIA (Throttling oparty na czasie symulacji) ---
last_print_sim_time = {
    "PRESS_WARN": 0.0,
    "PRESS_OK": 0.0,
    "TEMP_WARN": 0.0,
    "TEMP_OK": 0.0,
    "BREACH_ALERT": 0.0
}

INTERVAL_WARN = 5.0    # Ostrzeżenia o predykcjach awarii: co 5 sekund symulacji
INTERVAL_OK = 10.0     # Dążenie do stabilizacji: co 10 sekund symulacji
INTERVAL_BREACH = 3.0  # Krytyczne przekroczenie progu (KATASTROFA): co 3 sekundy symulacji

print("--- SELEKTYWNY ANALIZATOR ANTYAWARYJNY URUCHOMIONY ---")
print(f"Punkty stabilizacji: {TARGET_PRESS} MPa | {TARGET_TEMP} °C\n")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Błąd: {msg.error()}")
                break
            
        data = json.loads(msg.value().decode('utf-8'))
        current_time = data['metadata']['timestamp']
        current_press = data['primary_loop']['pressure_mpa']
        current_temp = data['primary_loop']['temp_hot_c']
        
        # Zapis do okna kroczącego
        press_history.append((current_time, current_press))
        temp_history.append((current_time, current_temp))
        
        # ========================================================
        # 1. NAJWIĘKSZE ALERTY: PRZEKROCZENIE PUNKTÓW KRYTYCZNYCH
        # ========================================================
        breached = []
        if current_press <= CRITICAL_PRESS_LOW:
            breached.append(f"!!! ALARM KRYTYCZNY !!! CIŚNIENIE SPADŁO PONIŻEJ MINIMUM! Aktualne: {current_press:.2f} MPa (Limit dolny: {CRITICAL_PRESS_LOW} MPa)")
        elif current_press >= CRITICAL_PRESS_HIGH:
            breached.append(f"!!! ALARM KRYTYCZNY !!! CIŚNIENIE PRZEKROCZYŁO MAKSIMUM! Aktualne: {current_press:.2f} MPa (Limit górny: {CRITICAL_PRESS_HIGH} MPa)")
        if current_temp >= CRITICAL_TEMP_HIGH:
            breached.append(f"!!! ALARM KRYTYCZNY !!! PRZEGRZANIE RDZENIA REAKTORA! Aktualna: {current_temp:.2f} °C (Limit górny: {CRITICAL_TEMP_HIGH} °C)")

        if breached:
            if current_time - last_print_sim_time["BREACH_ALERT"] >= INTERVAL_BREACH:
                print(f"\n####################################################################################################")
                print(f"ALERT CZERWONY — AWARIA SYSTEMÓW — {time.strftime('%H:%M:%S')}")
                for b in breached:
                    print(b)
                print(f"####################################################################################################\n")
                last_print_sim_time["BREACH_ALERT"] = current_time
            continue # Blokujemy dalsze analizy trendów, gdy reaktor fizycznie tonie

        # Obliczanie trendów
        if len(press_history) == history_size:
            t_old, p_old = press_history[0]
            dt = current_time - t_old
            
            if dt > 0:
                dp_dt = (current_press - p_old) / dt
                dtemp_dt = (current_temp - temp_history[0][1]) / dt
                
                press_dev = current_press - TARGET_PRESS
                temp_dev = current_temp - TARGET_TEMP

                # ========================================================
                # 2. SELEKTYWNA ANALIZA CIŚNIENIA
                # ========================================================
                if abs(press_dev) <= PRESS_STABLE_ZONE:
                    # Wyświetl komunikat o wejściu w strefę tylko raz
                    if not press_inside_stable_zone:
                        print(f"=======================================================================================================")
                        print(f"[SUKCES] Ciśnienie OSIĄGNĘŁO obszar stabilizacji! Aktualne: {current_press:.2f} MPa (Cel: {TARGET_PRESS} MPa, Margines: ±{PRESS_STABLE_ZONE})")
                        print(f"=======================================================================================================")
                        press_inside_stable_zone = True
                else:
                    # Resetujemy flagę, jeśli wypadliśmy poza obszar stabilizacji
                    press_inside_stable_zone = False
                    
                    # ODDALANIE SIĘ OD CELU -> wyłącznie czyste predykcje
                    if (press_dev > 0 and dp_dt > 0.001) or (press_dev < 0 and dp_dt < -0.001):
                        if current_time - last_print_sim_time["PRESS_WARN"] >= INTERVAL_WARN:
                            
                            # Spadki ciśnienia (np. LEAK)
                            if dp_dt < 0 and current_press > CRITICAL_PRESS_LOW:
                                time_to_crit = (CRITICAL_PRESS_LOW - current_press) / dp_dt
                                if dp_dt <= -0.05 and 0 < time_to_crit < 30:
                                    print(f"[PREDYKCJA - ZAGROŻENIE GWAŁTOWNE] Szybki spadek ciśnienia! Przekroczenie progu {CRITICAL_PRESS_LOW} MPa za {time_to_crit:.1f} s. (Trend: {dp_dt:.3f} MPa/s)")
                                    last_print_sim_time["PRESS_WARN"] = current_time
                                elif -0.05 < dp_dt <= -0.01 and 0 < time_to_crit < 90:
                                    print(f"[PREDYKCJA - ANOMALIA PEŁZAJĄCA] Długotrwały spadek ciśnienia! Przekroczenie progu {CRITICAL_PRESS_LOW} MPa za {time_to_crit:.1f} s. (Trend: {dp_dt:.3f} MPa/s)")
                                    last_print_sim_time["PRESS_WARN"] = current_time
                                    
                            # Wzrosty ciśnienia (np. TURBINE_TRIP)
                            elif dp_dt > 0 and current_press < CRITICAL_PRESS_HIGH:
                                time_to_crit = (CRITICAL_PRESS_HIGH - current_press) / dp_dt
                                if dp_dt >= 0.05 and 0 < time_to_crit < 30:
                                    print(f"[PREDYKCJA - ZAGROŻENIE GWAŁTOWNE] Ekstremalny skok ciśnienia! Rozsadzenie obiegu za {time_to_crit:.1f} s! (Trend: {dp_dt:+.3f} MPa/s)")
                                    last_print_sim_time["PRESS_WARN"] = current_time
                                elif 0.01 <= dp_dt < 0.05 and 0 < time_to_crit < 90:
                                    print(f"[PREDYKCJA - ANOMALIA PEŁZAJĄCA] Stały, powolny wzrost ciśnienia. Przekroczenie normy za {time_to_crit:.1f} s. (Trend: {dp_dt:+.3f} MPa/s)")
                                    last_print_sim_time["PRESS_WARN"] = current_time

                    # ZBLIŻANIE SIĘ DO CELU -> komunikat o dążeniu do stabilizacji
                    elif (press_dev > 0 and dp_dt < -0.001) or (press_dev < 0 and dp_dt > 0.001) and abs(dp_dt) > 0.1:
                        if current_time - last_print_sim_time["PRESS_OK"] >= INTERVAL_OK:
                            print(f"[INFO - STABILIZACJA] Ciśnienie dąży do wartości punktu stabilizacji ({TARGET_PRESS} MPa). Aktualne: {current_press:.2f} MPa | Trend: {dp_dt:+.3f} MPa/s")
                            last_print_sim_time["PRESS_OK"] = current_time

                # ========================================================
                # 3. SELEKTYWNA ANALIZA TEMPERATURY
                # ========================================================
                if abs(temp_dev) <= TEMP_STABLE_ZONE:
                    # Wyświetl komunikat o wejściu w strefę tylko raz
                    if not temp_inside_stable_zone:
                        print(f"=======================================================================================================")
                        print(f"[SUKCES] Temperatura OSIĄGNĘŁA obszar stabilizacji! Aktualna: {current_temp:.2f} °C (Cel: {TARGET_TEMP} °C, Margines: ±{TEMP_STABLE_ZONE})")
                        print(f"=======================================================================================================")
                        temp_inside_stable_zone = True
                else:
                    # Resetujemy flagę, jeśli wypadliśmy poza obszar stabilizacji
                    temp_inside_stable_zone = False
                    
                    # ODDALANIE SIĘ OD CELU -> wyłącznie czyste predykcje
                    if (temp_dev > 0 and dtemp_dt > 0.005) or (temp_dev < 0 and dtemp_dt < -0.005) and abs(dtemp_dt) > 0.1:
                        if current_time - last_print_sim_time["TEMP_WARN"] >= INTERVAL_WARN:
                            
                            if dtemp_dt > 0 and current_temp < CRITICAL_TEMP_HIGH:
                                time_to_crit = (CRITICAL_TEMP_HIGH - current_temp) / dtemp_dt
                                if dtemp_dt >= 0.4 and 0 < time_to_crit < 30:
                                    print(f"[PREDYKCJA - ZAGROŻENIE GWAŁTOWNE] Rdzeń błyskawicznie się nagrzewa! Przekroczenie {CRITICAL_TEMP_HIGH} C za {time_to_crit:.1f} s! (Trend: {dtemp_dt:+.2f} C/s)")
                                    last_print_sim_time["TEMP_WARN"] = current_time
                                elif 0.05 <= dtemp_dt < 0.4 and 0 < time_to_crit < 90:
                                    print(f"[PREDYKCJA - ANOMALIA PEŁZAJĄCA] Wykryto powolną akumulację ciepła w rdzeniu. Przegrzanie za {time_to_crit:.1f} s. (Trend: {dtemp_dt:+.2f} C/s)")
                                    last_print_sim_time["TEMP_WARN"] = current_time

                    # ZBLIŻANIE SIĘ DO CELU -> komunikat o dążeniu do stabilizacji
                    elif (temp_dev > 0 and dtemp_dt < -0.005) or (temp_dev < 0 and dtemp_dt > 0.005):
                        if current_time - last_print_sim_time["TEMP_OK"] >= INTERVAL_OK:
                            print(f"[INFO - STABILIZACJA] Temperatura dąży do wartości punktu stabilizacji ({TARGET_TEMP} °C). Aktualna: {current_temp:.2f} °C | Trend: {dtemp_dt:+.3f} °C/s")
                            last_print_sim_time["TEMP_OK"] = current_time

except KeyboardInterrupt:
    print("\nZamykanie konsumenta...")
finally:
    consumer.close()
