import json
import time
import random
import os
from confluent_kafka import Producer

# --- KONFIGURACJA ---
KAFKA_CONFIG = {'bootstrap.servers': 'broker:9092'}
TOPIC_NAME = 'nuclear-reactor-data'
CONTROL_FILE = "control.json"

producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    if err is not None:
        print(f"Błąd wysyłki: {err}")

# --- STAN POCZĄTKOWY (Parametry bazowe - 60% mocy) ---
state = {
    # Rdzeń (Core)
    "neutron_flux": 60.0,         # % mocy nominalnej
    "control_rods_out": 55.0,     # % Wysunięcie: 55% (Im pręty bardziej wysunięte, tym większa moc)
    "boron_concentration": 700,   # ppm - stężenie boru (hamulec chemiczny - im większe, tym wolniejsza reakcja)
    
    # Układ Pierwotny (Primary Loop) - obieg zamknięty z reaktorem
    "p_temp_hot": 305.0,          # C - temp. wody wychodzącej z reaktora
    "p_temp_cold": 285.0,         # C - temp. wody wracającej do reaktora
    "p_pressure": 15.1,           # MPa - ciśnienie wody w obiegu pierwotnym
    "p_flow_rate": 100.0,         # % - przepływ chłodziwa przez pompy RCP
    
    # Układ Wtórny (Secondary Loop) - obieg pary napędzającej turbinę
    "s_steam_pressure": 5.2,      # MPa - ciśnienie pary w wytwornicy prądu
    "s_steam_temp": 265.0,        # C - temperatura pary na turbinie
    "s_feedwater_flow": 1100.0,   # kg/s - ilość wody zasilającej obieg wtórny
    "turbine_rpm": 3000.0,        # obr/min - prędkość obrotowa turbiny generatora
    
    # Generator i Sieć
    "gen_output_mwe": 600.0,      # MW - moc elektryczna oddawana do sieci
    "grid_frequency": 50.00,      # Hz - częstotliwość sieci energetycznej
    
    # Bezpieczeństwo i Otoczenie
    "containment_pressure": 0.101, # MPa - ciśnienie wewnątrz kopuły bezpieczeństwa
    "radiation_level": 0.12,       # uSv/h - poziom promieniowania w budynku
    "ambient_temp": 22.0,          # C - temperatura otoczenia/powietrza
    
    "fault_mode": "NONE",          # zmienna sterująca awariami
    "safety_system": "NONE"        # zmienna sterująca systemami awaryjnymi
}



print(f"Uruchamiam zaawansowany symulator reaktora na topiku: {TOPIC_NAME}")

try:
    while True:
        # --- 0. ODCZYT KOMEND STERUJĄCYCH ---
        if os.path.exists(CONTROL_FILE):
            try:
                with open(CONTROL_FILE, "r") as f:
                    commands = json.load(f)
                    
                    # AKTUALIZACJA: Sprawdzamy czy system bezpieczeństwa jest aktywny
                    current_safety = commands.get("safety_system", state["safety_system"])
                    
                    # Jeśli nie ma SCRAM-u, pozwól na sterowanie prętami
                    if current_safety != "SCRAM":
                        if "rods_out" in commands:
                            state["control_rods_out"] = float(commands["rods_out"])
                        if "boron" in commands:
                            state["boron_concentration"] = max(0.0, float(commands["boron"]))
                    
                    if "fault" in commands:
                        state["fault_mode"] = commands["fault"]
                    if "safety_system" in commands:
                        state["safety_system"] = commands["safety_system"]
            except Exception: pass # Ignorujemy błędy, jeśli plik jest pusty lub w trakcie zapisu
        
        # --- LOGIKA SYMULACJI (ZALEŻNOŚCI FIZYCZNE) ---

        # 1. WPŁYW SIECI NA GENERATOR
        # Częstotliwość sieci lekko dryfuje, co zmusza turbinę do pracy (bezwładność)
        state["grid_frequency"] = max(0.0, 50.0 + random.uniform(-0.01, 0.01))
        state["turbine_rpm"] = state["grid_frequency"] * 60

        # 2. KINETYKA RDZENIA (WPŁYW PRĘTÓW I BORU)
        # Moc (neutron_flux) dąży do poziomu wyznaczonego przez wysunięcie prętów i stężenie boru
        # Wzór: target_flux rośnie z rods_out i maleje z boron_concentration
        target_flux = (state["control_rods_out"] * 1.2) - (state["boron_concentration"] / 100.0)
        state["neutron_flux"] += (target_flux - state["neutron_flux"]) * 0.05 + random.uniform(-0.02, 0.02)
        state["neutron_flux"] = max(0.0, state["neutron_flux"])

        # 3. TERMODYNAMIKA UKŁADU PIERWOTNEGO (PRIMARY LOOP)
        # Temperatura wody rośnie wraz z mocą (flux), ale jest chłodzona przez przepływ (flow)
        target_temp_hot = 270.0 + (state["neutron_flux"] * 0.6)
        state["p_temp_hot"] += (target_temp_hot - state["p_temp_hot"]) * 0.1
        state["p_temp_hot"] = max(0.0, state["p_temp_hot"])
        
        # Temp_cold zależy od tego, ile ciepła "zabrał" układ wtórny (delta T)
        state["p_temp_cold"] = max(0.0, state["p_temp_hot"] - (state["neutron_flux"] * 0.35))
        
        # Ciśnienie rośnie z temperaturą (rozszerzalność cieplna wody pod ciśnieniem)
        target_pressure = 14.0 + (state["p_temp_hot"] * 0.0035)
        state["p_pressure"] += (target_pressure - state["p_pressure"]) * 0.05 + random.uniform(-0.002, 0.002)
        state["p_pressure"] = max(0.0, state["p_pressure"])

        # 4. UKŁAD WTÓRNY (SECONDARY LOOP - WYTWORNICA PARY)
        # Ciśnienie pary zależy od temperatury p_temp_hot
        target_steam_press = max(0.0, (state["p_temp_hot"] - 270.0) * 0.15)
        state["s_steam_pressure"] += (target_steam_press - state["s_steam_pressure"]) * 0.05
        state["s_steam_pressure"] = max(0.0, state["s_steam_pressure"])
        
        # Temperatura pary jest skorelowana z jej ciśnieniem
        state["s_steam_temp"] = max(0.0, 240.0 + (state["s_steam_pressure"] * 5.0))
        state["s_feedwater_flow"] = max(0.0, state["neutron_flux"] * 18.5 + random.uniform(-2, 2))
        
        # Przepływ wody zasilającej (feedwater) musi nadążać za mocą, by nie osuszyć wytwornicy
        state["s_feedwater_flow"] = state["neutron_flux"] * 18.5 + random.uniform(-2, 2)

        # 5. WYJŚCIE ENERGETYCZNE (OUTPUT)
        # Moc elektryczna to sprawność układu (ok. 33%) razy moc cieplna
        state["gen_output_mwe"] = max(0.0, state["neutron_flux"] * 10.0 + random.uniform(-0.1, 0.1))

        # 6. BEZPIECZEŃSTWO (REAKCJA NA CIŚNIENIE I TEMP)
        # Promieniowanie lekko rośnie, gdy reaktor pracuje na bardzo wysokiej mocy
        if state["neutron_flux"] > 90:
            state["radiation_level"] += 0.0001
        else:
            state["radiation_level"] = max(0.12, state["radiation_level"] - 0.00005)

        # --- 7. SYMULACJA USTEREK ---
        if state["fault_mode"] == "LEAK":
            # Wyciek: Ciśnienie pierwotne spada, ciśnienie w kopule rośnie
            state["p_pressure"] = max(0.0, state["p_pressure"] - 0.05)
            state["containment_pressure"] += 0.005
            state["radiation_level"] += 0.02
            # Jeśli ciśnienie spadnie za nisko, woda wrze, szybko paruje i gorzej chłodzi:
            if state["p_pressure"] < 10.0:
                state["p_temp_hot"] += 1.5

        elif state["fault_mode"] == "PUMP_FAIL":
            # Awaria pompy: Przepływ spada do 20%, temperatura hot szybuje w górę
            state["p_flow_rate"] = max(20.0, state["p_flow_rate"] - 5.0)
            state["p_temp_hot"] += 2.0  # Bardzo szybki wzrost temp!

        elif state["fault_mode"] == "TURBINE_TRIP":
            # REAKCJA: Odłączenie sieci -> Wzrost ciśnienia pary -> Wzrost temp. reaktora
            state["grid_frequency"] = 52.5  # Nagły skok częstotliwości (brak obciążenia)
            state["s_steam_pressure"] += 0.5
            state["s_steam_temp"] += 5.0
            # Ciepło "cofa się" do reaktora:
            state["p_temp_hot"] += 0.8
            state["p_pressure"] += 0.1

        elif state["fault_mode"] == "TOTAL_BLACKOUT":
            # KATASTROFA: Całkowity brak prądu w elektrowni (SBO)
            # 1. Przepływ pierwotny gwałtownie spada do zera
            state["p_flow_rate"] = max(0.0, state["p_flow_rate"] - 15.0) 

            # 2. Układ wtórny całkowicie zamiera (pompy wody zasilającej stoją)
            state["s_feedwater_flow"] = max(0.0, state["s_feedwater_flow"] - 100.0)

            # 3. Ekstremalny wzrost temperatury (brak jakiegokolwiek odbioru ciepła)
            if state["p_flow_rate"] < 10.0:
                state["p_temp_hot"] += 8.0  # Reaktor dosłownie "gotuje się" w środku
                state["p_pressure"] += 1.2  # Ryzyko rozerwania rurociągów
                state["radiation_level"] += 0.1 # Natychmiastowe uszkodzenia paliwa

        # --- 8. LOGIKA SYSTEMÓW BEZPIECZEŃSTWA (safety_system) ---
        if state["safety_system"] == "SCRAM":
            # Gwałtowne wygaszanie - pręty w dół, flux w dół
            state["control_rods_out"] = max(0.0, state["control_rods_out"] - 20.0)
            state["neutron_flux"] = max(0.0, state["neutron_flux"] - 10.0)

        elif state["safety_system"] == "STEAM_DUMP":
            # Zrzut pary: gwałtowny spadek ciśnienia i temperatury
            state["p_pressure"] = max(1.0, state["p_pressure"] - 0.8)
            state["s_steam_pressure"] = max(0.5, state["s_steam_pressure"] - 1.5)
            state["p_temp_hot"] = max(0.0, state["p_temp_hot"] - 3.0)
            # Jeśli mamy wyciek, zrzut pary wyrzuca skażoną parę na zewnątrz
            if state["fault_mode"] == "LEAK":
                state["radiation_level"] += 0.05

        elif state["safety_system"] == "EMERGENCY_BORON":
            # Szybkie borowanie (chemiczne hamowanie reakcji)
            state["boron_concentration"] += 20.0

        # --- BUDOWANIE KOMUNIKATU ---
        payload = {
            "metadata": {
                "timestamp": time.time(),
                "sensor_group": "full_state",
                "reactor_id": "PWR-UNIT-01",
                "status": "OPERATIONAL"
            },
            "core": {
                "neutron_flux_pct": round(state["neutron_flux"], 3),
                "control_rods_out_pct": round(state["control_rods_out"], 2),
                "boron_ppm": int(state["boron_concentration"])
            },
            "primary_loop": {
                "temp_hot_c": round(state["p_temp_hot"], 2),
                "temp_cold_c": round(state["p_temp_cold"], 2),
                "pressure_mpa": round(state["p_pressure"], 3),
                "flow_pct": round(state["p_flow_rate"], 2)
            },
            "secondary_loop": {
                "steam_pressure_mpa": round(state["s_steam_pressure"], 2),
                "steam_temp_c": round(state["s_steam_temp"], 2),
                "feedwater_flow_kgs": round(state["s_feedwater_flow"], 2),
                "turbine_rpm": round(state["turbine_rpm"], 2)
            },
            "output": {
                "power_mwe": round(state["gen_output_mwe"], 2),
                "grid_freq_hz": round(state["grid_frequency"], 3)
            },
            "safety": {
                "containment_press_mpa": round(state["containment_pressure"], 4),
                "radiation_usvh": round(state["radiation_level"], 4),
                "ambient_temp_c": round(state["ambient_temp"], 1)
            }
        }

        # --- WYSYŁKA ---
        producer.produce(
            TOPIC_NAME,
            key=payload["metadata"]["reactor_id"],
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        
        producer.poll(0)
        producer.flush()

        # Zmienione: brak end='\r' spowoduje wypisywanie w nowych liniach
        print(f"[{time.strftime('%H:%M:%S')}] Core: {payload['core']['neutron_flux_pct']}% | Power: {payload['output']['power_mwe']}MW | Status: {payload['metadata']['status']}")
        
        time.sleep(1.0) # Częstotliwość próbkowania: 1Hz

except KeyboardInterrupt:
    print("\nWyłączanie symulatora...")
### KONTROLOWANIE REAKTORA ###
# trzeba w odzielnym termianlu wpisać poniższą komendę, ustawiając wartości rods_out i boron;
# rods_out: 0 - reaktor wygaszony, 100 - max moc, boron: tym więcej boru, tym spokojniejsza reakcja
# Kod: echo '{"rods_out": 60.0, "boron": 700}' > control.json
#
###  SYMULOWANIE USTEREK   ###
# aby zasymolować awarię, w odzielnym terminalu trzeba wpisać poniższą komendę, wybierająć rodzaj fault;
# echo '{"fault": "NONE"}' > control.json           - brak usterki
# echo '{"fault": "LEAK"}' > control.json           - Wyciek: Ciśnienie pierwotne spada, ciśnienie w kopule rośnie, promieniowanie rośnie 
# echo '{"fault": "PUMP_FAIL"}' > control.json      - Awaria pompy: Przepływ spada do 20%, temperatura hot szybuje w górę
# echo '{"fault": "TURBINE_TRIP"}' > control.json   - Odłączenie sieci -> Wzrost ciśnienia pary -> Wzrost temp. reaktora
# echo '{"fault": "TOTAL_BLACKOUT"}' > control.json - KATASTROFA: Całkowity brak prądu w elektrowni (SBO) -> Przepływ pierwotny gwałtownie spada do zera
#                                                         -> Układ wtórny całkowicie zamiera (pompy wody zasilającej stoją) -> Ekstremalny wzrost temperatury (brak odbioru ciepła)
###  SYSTEMY AWARYJNE   ###
# echo '{"safety_system": "NONE"}' > control.json             - systemy awaryjne wyłączone
# echo '{"safety_system": "STEAM_DUMP"}' > control.json       - Zrzut pary: gwałtowny spadek ciśnienia i temperatury -> Jeśli mamy wyciek, zrzut pary wyrzuca skażoną parę na zewnątrz
# echo '{"safety_system": "EMERGENCY_BORON"}' > control.json  - Szybkie borowanie (chemiczne hamowanie reakcji)
# echo '{"safety_system": "SCRAM"}' > control.json            - w ostateczności: całkowite zrzucenie prętów i wygasznie reaktora
# Jeśli włączymy system awaryjny to potem musimy go wyłączyć!
