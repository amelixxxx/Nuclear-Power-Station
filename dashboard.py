"""
Interaktywny Dashboard do Analizy Reaktora Jądrowego w Czasie Rzeczywistym
=========================================================================
Dashboard pobiera dane z Flask API (app.py) i wyświetla je za pomocą Streamlit.
Monitoring: parametry rdzenia, pętle chłodzące, bezpieczeństwo, anomalie.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import requests
import time
from datetime import datetime, timedelta
from collections import deque
import json

# ============================================================================
# KONFIGURACJA
# ============================================================================
API_BASE_URL = "http://localhost:5000"
MAX_HISTORY = 300  # Przechowuj ostatnie 5 minut (300 sekund) przy 1Hz
REFRESH_INTERVAL = 1  # Odśwież co 1 sekundę

# ============================================================================
# INICJALIZACJA CACHE'A STREAMLIT
# ============================================================================
if "reactor_history" not in st.session_state:
    st.session_state.reactor_history = deque(maxlen=MAX_HISTORY)

if "risk_scores" not in st.session_state:
    st.session_state.risk_scores = deque(maxlen=MAX_HISTORY)

if "last_update" not in st.session_state:
    st.session_state.last_update = None

if "alerts" not in st.session_state:
    st.session_state.alerts = deque(maxlen=50)

# ============================================================================
# FUNKCJE POMOCNICZE
# ============================================================================

def fetch_latest_stats():
    """Pobierz statystyki z API."""
    try:
        response = requests.get(f"{API_BASE_URL}/stats", timeout=2)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"❌ Błąd połączenia z API: {str(e)}")
    return None

def get_health_status():
    """Sprawdź status API."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=2)
        if response.status_code == 200:
            return response.json()
    except:
        return None

def calculate_risk_level_color(score):
    """Zwróć kolor na podstawie wyniku ryzyka."""
    if score <= 20:
        return "#00FF00"  # Zielony - NISKIE
    elif score <= 40:
        return "#FFFF00"  # Żółty - ŚREDNIE
    elif score <= 70:
        return "#FF6600"  # Pomarańczowy - WYSOKIE
    else:
        return "#FF0000"  # Czerwony - KRYTYCZNE

def calculate_risk_level_badge(score):
    """Zwróć etykietę poziomu ryzyka."""
    if score <= 20:
        return "🟢 NISKIE"
    elif score <= 40:
        return "🟡 ŚREDNIE"
    elif score <= 70:
        return "🟠 WYSOKIE"
    else:
        return "🔴 KRYTYCZNE"

def get_trend_indicator(current, previous):
    """Pokaż trend: ↑ / ↓ / →"""
    if previous is None:
        return "→"
    if current > previous * 1.02:
        return "↑"
    elif current < previous * 0.98:
        return "↓"
    else:
        return "→"

# ============================================================================
# INTERFEJS STREAMLIT
# ============================================================================

st.set_page_config(
    page_title="Nuclear Reactor Dashboard",
    page_icon="⚛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Nagłówek
st.markdown("""
<style>
    .header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-card {
        background: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
    }
    .alert-box {
        background: #ffe6e6;
        padding: 12px;
        border-radius: 5px;
        border-left: 4px solid #ff0000;
        margin: 5px 0;
    }
    .warning-box {
        background: #fff3cd;
        padding: 12px;
        border-radius: 5px;
        border-left: 4px solid #ffc107;
        margin: 5px 0;
    }
</style>
<div class="header">
    <h1>⚛️ REAKTOR PWR-UNIT-01 — MONITOR CZASU RZECZYWISTEGO</h1>
    <p>Analiza danych w czasie rzeczywistym | Real-time Data Analysis</p>
</div>
""", unsafe_allow_html=True)

# Sidebar - Konfiguracja
with st.sidebar:
    st.header("⚙️ Ustawienia")
    
    # Status API
    health = get_health_status()
    if health:
        st.success(f"✅ API Online | Wersja: {health.get('version', 'N/A')}")
        st.info(f"Przetworzonych zdarzeń: {health.get('events_processed', 0)}")
    else:
        st.error("❌ API Offline")
    
    # Opcje wyświetlania
    st.subheader("Opcje wykresu")
    time_window = st.selectbox(
        "Okno czasowe",
        options=["Ostatnia minuta (60s)", "Ostatnie 5 minut (300s)"],
        index=1
    )
    
    time_window_seconds = 60 if time_window == "Ostatnia minuta (60s)" else 300
    
    show_advanced = st.checkbox("Pokazuj widok zaawansowany", value=True)
    
    st.divider()
    st.subheader("📊 Informacje o systemie")
    st.text("• Język: Python\n• Framework: Streamlit\n• Wizualizacja: Plotly\n• API: Flask")

# Pobierz najnowsze dane
stats = fetch_latest_stats()

if stats and stats.get("total_events", 0) > 0:
    # ========================================================================
    # KPI - GŁÓWNE WSKAŹNIKI
    # ========================================================================
    st.subheader("🎯 Główne Wskaźniki Wydajności")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        avg_score = stats.get("avg_score", 0)
        st.metric(
            "Średni Wynik Ryzyka",
            f"{avg_score:.0f}",
            f"{avg_score - 25:.1f}",  # Delta
            delta_color="inverse"
        )
    
    with col2:
        max_score = stats.get("max_score", 0)
        st.metric(
            "Maks. Wynik",
            f"{max_score:.0f}",
            f"{calculate_risk_level_badge(max_score)}"
        )
    
    with col3:
        total_events = stats.get("total_events", 0)
        st.metric(
            "Razem Zdarzeń",
            f"{total_events:,}",
            f"+{total_events % 100}"
        )
    
    with col4:
        risk_dist = stats.get("risk_distribution", {})
        critical_count = risk_dist.get("CRITICAL", 0)
        high_count = risk_dist.get("HIGH", 0)
        st.metric(
            "🔴 Krytyczne/Wysokie",
            f"{critical_count + high_count}",
            f"CRITICAL: {critical_count}",
            delta_color="inverse"
        )
    
    with col5:
        low_count = risk_dist.get("LOW", 0)
        safe_pct = (low_count / total_events * 100) if total_events > 0 else 0
        st.metric(
            "🟢 Bezpieczne [%]",
            f"{safe_pct:.1f}%",
            f"+{safe_pct - 50:.1f}%"
        )
    
    st.divider()
    
    # ========================================================================
    # ROZKŁAD POZIOMÓW RYZYKA
    # ========================================================================
    st.subheader("📈 Rozkład Poziomów Ryzyka")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart
        risk_dist = stats.get("risk_distribution", {})
        fig_pie = go.Figure(
            data=[go.Pie(
                labels=list(risk_dist.keys()),
                values=list(risk_dist.values()),
                marker=dict(colors=["#00FF00", "#FFFF00", "#FF6600", "#FF0000"]),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Ilość: %{value}<br>Procent: %{percent}<extra></extra>"
            )]
        )
        fig_pie.update_layout(
            title="Rozkład Zdarzeń po Poziomach Ryzyka",
            height=400,
            showlegend=True
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Bar chart
        rules = stats.get("top_triggered_rules", [])
        if rules:
            rule_names = [r["rule"][:20] for r in rules]
            rule_counts = [r["count"] for r in rules]
            
            fig_bar = go.Figure(
                data=[go.Bar(
                    x=rule_counts,
                    y=rule_names,
                    orientation="h",
                    marker=dict(color=rule_counts, colorscale="Reds"),
                    text=rule_counts,
                    textposition="auto",
                    hovertemplate="<b>%{y}</b><br>Wyzwolenia: %{x}<extra></extra>"
                )]
            )
            fig_bar.update_layout(
                title="Top 10 Wyzwolonych Reguł",
                xaxis_title="Liczba Wyzwoleń",
                yaxis_title="Reguła",
                height=400,
                margin=dict(l=250)
            )
            st.plotly_chart(fig_bar, use_container_width=True)
    
    st.divider()
    
    # ========================================================================
    # OSTATNIE ZDARZENIA
    # ========================================================================
    st.subheader("📋 Ostatnie Zdarzenia (Top 5)")
    
    recent = stats.get("recent_events", [])
    if recent:
        recent_df = pd.DataFrame(recent)
        
        # Formatuj DF
        display_cols = ["reactor_id", "risk_level", "score", "pressure_mpa", "temp_hot_c", "radiation_usvh", "flow_pct"]
        display_df = recent_df[display_cols].copy()
        
        # Pokoloruj wiersze wg ryzyka
        def color_risk(val):
            if val == "LOW":
                return "background-color: #00FF00; color: black"
            elif val == "MEDIUM":
                return "background-color: #FFFF00; color: black"
            elif val == "HIGH":
                return "background-color: #FF6600; color: white"
            else:
                return "background-color: #FF0000; color: white"
        
        styled_df = display_df.style.applymap(
            lambda x: color_risk(x) if x in ["LOW", "MEDIUM", "HIGH", "CRITICAL"] else "",
            subset=["risk_level"]
        )
        
        st.dataframe(styled_df, use_container_width=True)
    
    st.divider()
    
    if show_advanced:
        st.subheader("🔬 Widok Zaawansowany")
        
        # Tabelka statystyk
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Min. Wynik", stats.get("min_score", 0))
        with col2:
            st.metric("Max. Wynik", stats.get("max_score", 0))
        with col3:
            st.metric("Średni Wynik", f"{stats.get('avg_score', 0):.2f}")
        
        # Szczegółowy JSON
        with st.expander("📡 Pełna Odpowiedź API (JSON)"):
            st.json(stats)
else:
    st.warning("⏳ Oczekiwanie na dane z API... Upewnij się, że aplikacja Flask i symulator działają.")
    st.info("""
    Aby uruchomić system:
    
    1. **Terminal 1** - Producer (symulator):
       ```bash
       python producer.py
       ```
    
    2. **Terminal 2** - API Scoring:
       ```bash
       python app.py
       ```
    
    3. **Terminal 3** - Consumer (Spark):
       ```bash
       python spark_consumer.py
       ```
    
    4. **Terminal 4** - Dashboard:
       ```bash
       streamlit run dashboard.py
       ```
    """)

# Auto-refresh
st.markdown("""
<script>
    // Automatycznie odśwież co 1 sekundę
    setTimeout(function() {
        window.parent.document.querySelector('button[kind="secondary"]').click();
    }, 1000);
</script>
""", unsafe_allow_html=True)

st.divider()
st.caption(f"⏰ Ostatnia aktualizacja: {datetime.now().strftime('%H:%M:%S')} | Dashboard v1.0")
