# ⚛️ Dashboard do Analizy Reaktora Jądrowego w Czasie Rzeczywistym

**Real-time Nuclear Reactor Monitoring Dashboard**

## 📊 Opis

Interaktywny dashboard do monitorowania parametrów reaktora jądrowego w czasie rzeczywistym. System analizuje dane przesyłane przez symulator reaktora PWR, ocenia ryzyko i wyświetla wizualizacje za pomocą biblioteki **Plotly** i **Streamlit**.

## 🎯 Główne Funkcjonalności

### 1. **Monitorowanie KPI** (Key Performance Indicators)
- 📈 Średni wynik ryzyka
- 🔴 Maksymalny wynik ryzyka
- 📊 Liczba przetworzonych zdarzeń
- 🚨 Liczba zdarzeń krytycznych/wysokich
- 🟢 Procent zdarzeń bezpiecznych

### 2. **Wizualizacje Danych**
- 🥧 **Pie Chart** — Rozkład poziomów ryzyka (LOW/MEDIUM/HIGH/CRITICAL)
- 📊 **Bar Chart** — Top 10 wyzwolonych reguł alarmowych
- 📋 **Tabela zdarzeń** — Ostatnie 5 zdarzeń z kolorową indicacją poziomu ryzyka

### 3. **Status API**
- ✅ Sprawdzenie dostępności Flask API
- 🔍 Wyświetlanie liczby przetworzonych zdarzeń
- ⏰ Czasowe znaczniki aktualizacji

### 4. **Opcje Konfiguracyjne** (Sidebar)
- ⏱️ Wybór okna czasowego (1 min / 5 min)
- 👁️ Widok zaawansowany (min/max/średnia, JSON)
- 📊 Informacje o systemie

## 🛠️ Architektura

```
┌─────────────────────────────────────────────────┐
│         PRODUCER (symulator reaktora)            │
│  producer.py — generuje dane Kafki (1 Hz)       │
└──────────────────┬──────────────────────────────┘
                   │
            KAFKA TOPIC
     nuclear-reactor-data
                   │
        ┌──────────┴──────────┐
        │                     │
┌───────▼─────────┐  ┌────────▼────────┐
│ CONSUMER (Spark) │  │  FLASK API      │
│ spark_consumer   │  │  app.py         │
│                  │  │ /score endpoint │
└──────────────────┘  └────────┬────────┘
                               │
                        HTTP GET /stats
                               │
                    ┌──────────▼──────────┐
                    │  DASHBOARD (TY!)    │
                    │  dashboard.py       │
                    │  Streamlit + Plotly │
                    └─────────────────────┘
```

## 📦 Wymagania

Zainstaluj dependencje:

```bash
pip install -r requirements.txt
```

**Kluczowe pakiety:**
- `streamlit` — Frontend interaktywny
- `plotly` — Zaawansowane wykresy
- `pandas` — Przetwarzanie danych
- `requests` — Komunikacja z API
- `confluent-kafka` — Konsumpcja z Kafki
- `flask` — Framework API
- `pyspark` — Przetwarzanie stream'ów

## 🚀 Uruchomienie

### Krok 1: Uruchom Producer (symulator reaktora)
```bash
python producer.py
```
Generuje dane reaktora do topiku Kafki `nuclear-reactor-data`.

### Krok 2: Uruchom Flask API (Scoring)
```bash
python app.py
```
Endpoint: `http://localhost:5000`
- `POST /score` — Ocena ryzyka zdarzeń
- `GET /stats` — Statystyki historyczne
- `GET /health` — Status liveness

### Krok 3: Uruchom Consumer (opcjonalnie, do procesowania Spark)
```bash
python spark_consumer.py
```

### Krok 4: Uruchom Dashboard 🎉
```bash
streamlit run dashboard.py
```
Otwórz `http://localhost:8501` w przeglądarce.

## 📊 Ekran Główny

```
┌─────────────────────────────────────────────────────────┐
│ ⚛️  REAKTOR PWR-UNIT-01 — MONITOR CZASU RZECZYWISTEGO  │
│    Analiza danych w czasie rzeczywistym                  │
└─────────────────────────────────────────────────────────┘

🎯 GŁÓWNE WSKAŹNIKI WYDAJNOŚCI
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│ Średni    │ Maks.    │ Razem    │ 🔴       │ 🟢       │
│ Wynik: 35│ Wynik: 62│ Zdarzeń: │ Krytycz/ │ Bezpieczn│
│          │          │ 1,234    │ Wysokie: │ e: 75.3% │
└──────────┴──────────┴──────────┴──────────┴──────────┘

📈 ROZKŁAD POZIOMÓW RYZYKA         TOP 10 WYZWOLONYCH REGUŁ
┌──────────────────────────────┐  ┌─────────────────────┐
│  🟢 LOW: 45%  (567)          │  │ PRESSURE_LOW: 234   │
│  🟡 MEDIUM: 35% (432)        │  │ TEMP_HIGH: 187      │
│  🟠 HIGH: 15% (185)          │  │ FLOW_LOW: 156       │
│  🔴 CRITICAL: 5% (62)        │  │ ...                 │
└──────────────────────────────┘  └─────────────────────┘

📋 OSTATNIE ZDARZENIA (TOP 5)
┌──────────┬───────────┬───────┬──────────┬──────────┐
│ Reactor  │ Risk Lvl  │ Score │ Pressure │ Temp     │
│ PWR-01   │ 🟠 HIGH   │ 58    │ 15.2 MPa │ 325.1°C  │
│ PWR-01   │ 🟡 MEDIUM │ 38    │ 15.0 MPa │ 312.5°C  │
│ PWR-01   │ 🟢 LOW    │ 12    │ 14.9 MPa │ 305.0°C  │
└──────────┴───────────┴───────┴──────────┴──────────┘
```

## 🎨 Elementy Interfejsu

### Sidebar
- **Status API** — Wskaźnik dostępności
- **Okno czasowe** — Selektor 1 min / 5 min
- **Widok zaawansowany** — Toggle dla szczegółów
- **Info systemowa** — Technologia stack

### Główny obszar
- **KPI Cards** — 5 głównych metryk
- **Pie Chart** — Rozkład ryzyka
- **Bar Chart** — Top reguły alarmowe
- **Data Table** — Ostatnie zdarzenia
- **Advanced View** — Min/Max/Średnia + JSON

## 🔴 Poziomy Ryzyka

| Poziom | Zakres Punktów | Kolor | Znaczenie |
|--------|----------------|-------|-----------|
| **LOW** | 0–20 | 🟢 Zielony | Bezpieczna operacja |
| **MEDIUM** | 21–40 | 🟡 Żółty | Monitoring wymagany |
| **HIGH** | 41–70 | 🟠 Pomarańczowy | Interwencja zalecana |
| **CRITICAL** | 71+ | 🔴 Czerwony | Natychmiastowa akcja |

## 📡 Integracja z API

Dashboard komunikuje się z Flask API poprzez:

```python
# Pobierz statystyki
GET http://localhost:5000/stats

# Odpowiedź:
{
    "total_events": 1234,
    "risk_distribution": {
        "LOW": 567,
        "MEDIUM": 432,
        "HIGH": 185,
        "CRITICAL": 62
    },
    "avg_score": 35.2,
    "max_score": 62,
    "min_score": 5,
    "top_triggered_rules": [
        {"rule": "PRESSURE_WARN_LOW", "count": 234},
        {"rule": "TEMP_WARN_HIGH", "count": 187}
    ],
    "recent_events": [...]
}
```

## 🔄 Auto-Refresh

Dashboard automatycznie odświeża się co 1 sekundę, aby wyświetlać najnowsze dane z API.

## 🐛 Troubleshooting

### ❌ "API Offline"
- Sprawdź, czy `python app.py` jest uruchomiony
- Wyświetl logi: `curl http://localhost:5000/health`

### ❌ "Brak danych na wykresach"
- Uruchom `python producer.py` (producer musi generować dane)
- Czekaj ~30 sekund na nazbieranie się historii

### ❌ Port 8501 zajęty
```bash
streamlit run dashboard.py --server.port 8502
```

## 📚 Struktura Kodu

```
dashboard.py
├── Konfiguracja (API URL, MAX_HISTORY)
├── Session State (cache'owanie danych)
├── Funkcje Pomocnicze
│   ├── fetch_latest_stats()
│   ├── get_health_status()
│   ├── calculate_risk_level_color()
│   └── calculate_risk_level_badge()
├── Interfejs Streamlit
│   ├── Nagłówek
│   ├── Sidebar (status, opcje)
│   └── Zawartość główna
│       ├── KPI Cards
│       ├── Visualizations (Pie + Bar)
│       ├── Recent Events Table
│       └── Advanced View
└── Auto-Refresh Script
```

## 🎓 Instrukcje Laboratorium

1. **Część 1** — Uruchom symulator i API
   ```bash
   # Terminal 1
   python producer.py
   
   # Terminal 2
   python app.py
   ```

2. **Część 2** — Otwórz dashboard
   ```bash
   # Terminal 3
   streamlit run dashboard.py
   ```

3. **Część 3** — Symuluj awarie
   ```bash
   # Terminal 4
   echo '{"fault": "LEAK"}' > control.json
   # Obserwuj wykresy na dashboardzie!
   
   echo '{"safety_system": "SCRAM"}' > control.json
   # Reaktor się wyłącza
   ```

## 📈 Przykładowe Scenariusze

### Scenariusz 1: Normalna operacja
- Producer generuje dane z flux ~60%
- Wynik ryzyka: 10–20 (🟢 LOW)
- Wykres: flat line na dole

### Scenariusz 2: Ostrzeżenia
- Temperatura przekracza 320°C
- Ciśnienie spada poniżej 14.5 MPa
- Wynik ryzyka: 30–50 (🟡 MEDIUM / 🟠 HIGH)
- Wykres: kolorowe „igły" na dole

### Scenariusz 3: Kryzys
- LEAK + brak SCRAM
- Temperatura 340°C, ciśnienie 12 MPa
- Wynik ryzyka: 80+ (🔴 CRITICAL)
- Alarm audio/wizualny na dashboardzie

## 📝 Notatki

- Dashboard jest **read-only** — nie steruje reaktorem bezpośrednio
- Sterowanie: edytuj `control.json`, aby zmienić parametry symulatora
- Data przechowywana w RAM (resetuje się przy restarcie API)
- Historyczne dane: ostatnie 300 zdarzeń (~ 5 minut przy 1 Hz)

## 🔗 Powiązane Pliki

- `app.py` — Flask API (Scoring Engine)
- `producer.py` — Symulator reaktora
- `spark_consumer.py` — Stream processing (Spark)
- `Consumer.ipynb` — Notatnik konsumera Kafki

## 📞 Wsparcie

W razie problemów:
1. Sprawdź logi w terminalach
2. Upewnij się, że porty 5000 (API) i 8501 (Streamlit) są wolne
3. Sprawdź połączenie z Kafką (`broker:9092`)

---

**Wersja:** 1.0  
**Data utworzenia:** 2026-06-04  
**Autor:** Osoba 4 (Dashboard)  
**Projekt:** Nuclear Power Station — Real-time Data Analysis
