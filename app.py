"""
Flask API — Osoba 4 — Nuclear Reactor Scoring
Endpointy: POST /score | GET /stats | GET /health
"""

from flask import Flask, request, jsonify
import statistics
from datetime import datetime

app = Flask(__name__)

# Historia wszystkich zdarzeń (w pamięci RAM — czyści się przy restarcie)
event_history = []

# =============================================================================
# REGUŁY SCORINGU — każda reguła to jeden warunek alarmowy
# Gdy warunek jest spełniony, do sumy ryzyka (score) dodawane są punkty.
# =============================================================================
RULES = [
    # --- CIŚNIENIE (norma: 14.5 – 16.0 MPa) ---
    {
        "id": "PRESSURE_CRITICAL_LOW",
        "description": "Ciśnienie poniżej 13 MPa — możliwy wyciek chłodziwa (LOCA)",
        "check": lambda d: d.get("pressure_mpa", 15) < 13.0,
        "points": 40,
    },
    {
        "id": "PRESSURE_WARN_LOW",
        "description": "Ciśnienie poniżej 14.5 MPa — zbliżanie się do strefy wyciekowej",
        "check": lambda d: 13.0 <= d.get("pressure_mpa", 15) < 14.5,
        "points": 15,
    },
    {
        "id": "PRESSURE_CRITICAL_HIGH",
        "description": "Ciśnienie powyżej 17 MPa — ryzyko rozsadzenia obiegu pierwotnego",
        "check": lambda d: d.get("pressure_mpa", 15) > 17.0,
        "points": 40,
    },
    {
        "id": "PRESSURE_WARN_HIGH",
        "description": "Ciśnienie powyżej 15.5 MPa — wzrost ponad normę",
        "check": lambda d: 15.5 < d.get("pressure_mpa", 15) <= 17.0,
        "points": 10,
    },
    # --- TEMPERATURA RDZENIA (norma: 290–315 °C) ---
    {
        "id": "TEMP_CRITICAL_HIGH",
        "description": "Temperatura rdzenia powyżej 340 °C — przegrzanie krytyczne, ryzyko stopienia",
        "check": lambda d: d.get("temp_hot_c", 305) > 340.0,
        "points": 45,
    },
    {
        "id": "TEMP_WARN_HIGH",
        "description": "Temperatura rdzenia powyżej 320 °C — zbliżanie się do granicy bezpieczeństwa",
        "check": lambda d: 320.0 < d.get("temp_hot_c", 305) <= 340.0,
        "points": 20,
    },
    # --- STRUMIEŃ NEUTRONOWY — moc reaktora (norma: 20–90%) ---
    {
        "id": "NEUTRON_FLUX_CRITICAL",
        "description": "Strumień neutronów powyżej 95% — reaktor na granicy maksymalnej mocy",
        "check": lambda d: d.get("neutron_flux_pct", 60) > 95.0,
        "points": 30,
    },
    {
        "id": "NEUTRON_FLUX_HIGH",
        "description": "Strumień neutronów powyżej 85% — wysoka moc, wzmożony nadzór",
        "check": lambda d: 85.0 < d.get("neutron_flux_pct", 60) <= 95.0,
        "points": 10,
    },
    # --- PRZEPŁYW CHŁODZIWA (norma: 80–100%) ---
    {
        "id": "FLOW_CRITICAL_LOW",
        "description": "Przepływ chłodziwa poniżej 40% — poważna awaria chłodzenia rdzenia",
        "check": lambda d: d.get("flow_pct", 100) < 40.0,
        "points": 45,
    },
    {
        "id": "FLOW_WARN_LOW",
        "description": "Przepływ chłodziwa poniżej 70% — degradacja chłodzenia",
        "check": lambda d: 40.0 <= d.get("flow_pct", 100) < 70.0,
        "points": 20,
    },
    # --- PROMIENIOWANIE (norma: 0.10–0.13 µSv/h) ---
    {
        "id": "RADIATION_CRITICAL",
        "description": "Promieniowanie powyżej 0.5 µSv/h — poważny wyciek radioaktywności",
        "check": lambda d: d.get("radiation_usvh", 0.12) > 0.50,
        "points": 35,
    },
    {
        "id": "RADIATION_ELEVATED",
        "description": "Promieniowanie powyżej 0.20 µSv/h — podwyższony poziom, obserwacja",
        "check": lambda d: 0.20 < d.get("radiation_usvh", 0.12) <= 0.50,
        "points": 15,
    },
    # --- MOC ELEKTRYCZNA ---
    {
        "id": "POWER_ZERO",
        "description": "Moc elektryczna = 0 MW — turbina odłączona lub całkowity blackout (SBO)",
        "check": lambda d: d.get("power_mwe", 600) < 10.0,
        "points": 30,
    },
]


def calculate_risk_score(data: dict) -> dict:
    """Sprawdza wszystkie reguły i zwraca ocenę ryzyka."""
    score = 0
    triggered_rules = []
    details = []

    for rule in RULES:
        try:
            if rule["check"](data):
                score += rule["points"]
                triggered_rules.append(rule["id"])
                details.append(rule["description"])
        except Exception:
            pass

    # Przeliczanie sumy punktów na poziom ryzyka
    if score <= 20:
        risk_level = "LOW"
    elif score <= 40:
        risk_level = "MEDIUM"
    elif score <= 70:
        risk_level = "HIGH"
    else:
        risk_level = "CRITICAL"

    return {
        "score": score,
        "risk_level": risk_level,
        "triggered_rules": triggered_rules,
        "details": details,
        "timestamp_api": datetime.utcnow().isoformat() + "Z",
    }


@app.route("/score", methods=["POST"])
def score():
    """
    Przyjmuje jedno zdarzenie z reaktora, zwraca ocenę ryzyka.
    Używany przez Spark (Osoba 3) — każde zdarzenie z Kafki tu trafia.

    Przykład żądania:
    {
        "reactor_id": "PWR-UNIT-01",
        "neutron_flux_pct": 60.0,
        "pressure_mpa": 15.1,
        "temp_hot_c": 305.0,
        "flow_pct": 100.0,
        "radiation_usvh": 0.12,
        "power_mwe": 600.0
    }
    """
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Brak danych JSON w żądaniu"}), 400

    result = calculate_risk_score(data)
    result["reactor_id"] = data.get("reactor_id", "UNKNOWN")

    # Zapis do historii (do /stats)
    event_history.append({
        "reactor_id":      result["reactor_id"],
        "score":           result["score"],
        "risk_level":      result["risk_level"],
        "triggered_rules": result["triggered_rules"],
        "pressure_mpa":    data.get("pressure_mpa"),
        "temp_hot_c":      data.get("temp_hot_c"),
        "radiation_usvh":  data.get("radiation_usvh"),
        "flow_pct":        data.get("flow_pct"),
        "received_at":     result["timestamp_api"],
    })

    # Log w terminalu
    level = result["risk_level"]
    if level in ("HIGH", "CRITICAL"):
        print(f"  [!!!] {level:8s} score={result['score']:3d} | "
              f"{result['reactor_id']} | Reguły: {result['triggered_rules']}")
    else:
        print(f"  [ OK] {level:8s} score={result['score']:3d} | "
              f"{result['reactor_id']} | "
              f"P={data.get('pressure_mpa','?'):.2f}MPa "
              f"T={data.get('temp_hot_c','?'):.1f}°C")

    return jsonify(result), 200


@app.route("/stats", methods=["GET"])
def stats():
    """Zwraca statystyki historyczne od startu API."""
    if not event_history:
        return jsonify({
            "total_events": 0,
            "message": "Brak zdarzeń od startu API — czekam na dane z reaktora"
        }), 200

    scores = [e["score"] for e in event_history]

    risk_distribution = {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "CRITICAL": 0}
    for e in event_history:
        risk_distribution[e.get("risk_level", "LOW")] = \
            risk_distribution.get(e.get("risk_level", "LOW"), 0) + 1

    rule_counts = {}
    for e in event_history:
        for rule in e.get("triggered_rules", []):
            rule_counts[rule] = rule_counts.get(rule, 0) + 1

    top_rules = sorted(
        [{"rule": k, "count": v} for k, v in rule_counts.items()],
        key=lambda x: x["count"], reverse=True
    )[:10]

    return jsonify({
        "total_events":       len(event_history),
        "risk_distribution":  risk_distribution,
        "avg_score":          round(statistics.mean(scores), 2),
        "max_score":          max(scores),
        "min_score":          min(scores),
        "top_triggered_rules": top_rules,
        "recent_events":      event_history[-5:],
    }), 200


@app.route("/health", methods=["GET"])
def health():
    """Liveness check — używany przez Spark (Osoba 3)."""
    return jsonify({
        "status":            "ok",
        "service":           "Nuclear Reactor Scoring API — Osoba 4",
        "version":           "1.0",
        "events_processed":  len(event_history),
        "timestamp":         datetime.utcnow().isoformat() + "Z",
    }), 200


if __name__ == "__main__":
    print("=" * 60)
    print("  NUCLEAR REACTOR SCORING API — Osoba 4")
    print("=" * 60)
    print("  POST http://localhost:5000/score   — scoring zdarzenia")
    print("  GET  http://localhost:5000/stats   — statystyki")
    print("  GET  http://localhost:5000/health  — liveness check")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=False)
