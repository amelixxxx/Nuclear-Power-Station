import sys
import json
import requests
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp, window,
    count, avg, max as _max, min as _min,
    round as _round, lit, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType
)

# ─────────────────────────────────────────────
# KONFIGURACJA
# ─────────────────────────────────────────────
KAFKA_BROKER    = "broker:9092"
KAFKA_TOPIC     = "nuclear-reactor-data"
FLASK_API_URL   = "http://localhost:5000/score"
FLASK_HEALTH    = "http://localhost:5000/health"
WATERMARK       = "5 seconds"
WINDOW_DURATION = "30 seconds"

# ─────────────────────────────────────────────
# SCHEMAT JSON Z KAFKI (zagnieżdżony)
# ─────────────────────────────────────────────
metadata_schema = StructType([
    StructField("timestamp",    DoubleType()),
    StructField("sensor_group", StringType()),
    StructField("reactor_id",   StringType()),
    StructField("status",       StringType()),
])

core_schema = StructType([
    StructField("neutron_flux_pct",     DoubleType()),
    StructField("control_rods_out_pct", DoubleType()),
    StructField("boron_ppm",            IntegerType()),
])

primary_loop_schema = StructType([
    StructField("temp_hot_c",   DoubleType()),
    StructField("temp_cold_c",  DoubleType()),
    StructField("pressure_mpa", DoubleType()),
    StructField("flow_pct",     DoubleType()),
])

secondary_loop_schema = StructType([
    StructField("steam_pressure_mpa",  DoubleType()),
    StructField("steam_temp_c",        DoubleType()),
    StructField("feedwater_flow_kgs",  DoubleType()),
    StructField("turbine_rpm",         DoubleType()),
])

output_schema = StructType([
    StructField("power_mwe",    DoubleType()),
    StructField("grid_freq_hz", DoubleType()),
])

safety_schema = StructType([
    StructField("containment_press_mpa", DoubleType()),
    StructField("radiation_usvh",        DoubleType()),
    StructField("ambient_temp_c",        DoubleType()),
])

full_schema = StructType([
    StructField("metadata",       metadata_schema),
    StructField("core",           core_schema),
    StructField("primary_loop",   primary_loop_schema),
    StructField("secondary_loop", secondary_loop_schema),
    StructField("output",         output_schema),
    StructField("safety",         safety_schema),
])

# ─────────────────────────────────────────────
# FUNKCJA: WYŚWIETLANIE (Etap 4 — bez API)
# ─────────────────────────────────────────────
def display_batch(df, batch_id):
    print(f"\n{'='*70}")
    print(f"  Batch ID: {batch_id}")
    print(f"{'='*70}")
    df.show(truncate=False)

# ─────────────────────────────────────────────
# FUNKCJA: WYSYŁKA DO FLASK API (Etap 6)
# ─────────────────────────────────────────────
def send_to_api(df, batch_id):
    rows = df.collect()
    if not rows:
        print(f"[Batch {batch_id}] Brak danych.")
        return

    print(f"\n[Batch {batch_id}] Przetwarzam {len(rows)} zdarzeń...")

    for row in rows:
        payload = {
            "reactor_id":       row["reactor_id"],
            "timestamp":        str(row["event_time"]),
            "neutron_flux_pct": row["neutron_flux_pct"],
            "pressure_mpa":     row["pressure_mpa"],
            "temp_hot_c":       row["temp_hot_c"],
            "flow_pct":         row["flow_pct"],
            "radiation_usvh":   row["radiation_usvh"],
            "power_mwe":        row["power_mwe"],
        }
        try:
            response = requests.post(FLASK_API_URL, json=payload, timeout=3)
            result   = response.json()
            level    = result.get("risk_level", "???")
            score    = result.get("score", "?")
            rules    = result.get("triggered_rules", [])

            if level in ("HIGH", "CRITICAL"):
                print(f"  WARNING [{level}] score={score} | "
                      f"P={payload['pressure_mpa']:.2f}MPa "
                      f"T={payload['temp_hot_c']:.1f}C "
                      f"Rad={payload['radiation_usvh']:.4f} | "
                      f"Reguly: {rules}")
            else:
                print(f"  OK [{level}] score={score} | "
                      f"P={payload['pressure_mpa']:.2f}MPa "
                      f"T={payload['temp_hot_c']:.1f}C")

        except requests.exceptions.ConnectionError:
            print(f"  BLAD: API niedostepne ({FLASK_API_URL}) — czy Flask jest uruchomiony?")
        except Exception as e:
            print(f"  BLAD wysylki: {e}")

# ─────────────────────────────────────────────
# GŁÓWNA LOGIKA
# ─────────────────────────────────────────────
def main(mode: str):
    spark = (
        SparkSession.builder
        .appName("NuclearReactor-SparkConsumer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} gotowy | Tryb: {mode}")

    # Odczyt z Kafki
    kafka_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Dekodowanie
    parsed = (
        kafka_raw
        .select(from_json(col("value").cast("string"), full_schema).alias("d"))
        .select(
            to_timestamp(from_unixtime(col("d.metadata.timestamp").cast("long"))).alias("event_time"),
            col("d.metadata.reactor_id").alias("reactor_id"),
            col("d.metadata.status").alias("status"),
            col("d.core.neutron_flux_pct"),
            col("d.core.control_rods_out_pct"),
            col("d.core.boron_ppm"),
            col("d.primary_loop.temp_hot_c"),
            col("d.primary_loop.temp_cold_c"),
            col("d.primary_loop.pressure_mpa"),
            col("d.primary_loop.flow_pct"),
            col("d.secondary_loop.steam_pressure_mpa"),
            col("d.secondary_loop.steam_temp_c"),
            col("d.secondary_loop.feedwater_flow_kgs"),
            col("d.secondary_loop.turbine_rpm"),
            col("d.output.power_mwe"),
            col("d.output.grid_freq_hz"),
            col("d.safety.containment_press_mpa"),
            col("d.safety.radiation_usvh"),
            col("d.safety.ambient_temp_c"),
        )
    )

    if mode == "stream":
        # ETAP 4: samo wyświetlanie
        query = (
            parsed.writeStream
            .outputMode("append")
            .foreachBatch(display_batch)
            .option("checkpointLocation", "/tmp/checkpoint_stream")
            .start()
        )
        print("Strumien uruchomiony — wyswietlam zdarzenia z reaktora...")

    else:
        # ETAP 6: wysyłka do Flask API
        try:
            r = requests.get(FLASK_HEALTH, timeout=3)
            print(f"Flask API dostepne: {r.json()}")
        except Exception:
            print(f"UWAGA: Flask API niedostepne! Uruchom app.py (Osoba 4) najpierw.")

        # Okna agregacyjne 30s per reaktor
        windowed = (
            parsed
            .withWatermark("event_time", WATERMARK)
            .groupBy(window("event_time", WINDOW_DURATION), "reactor_id")
            .agg(
                _round(avg("neutron_flux_pct"), 2).alias("avg_flux"),
                _round(avg("pressure_mpa"),     3).alias("avg_pressure"),
                _round(_max("pressure_mpa"),    3).alias("max_pressure"),
                _round(_min("pressure_mpa"),    3).alias("min_pressure"),
                _round(avg("temp_hot_c"),        2).alias("avg_temp_hot"),
                _round(_max("temp_hot_c"),       2).alias("max_temp_hot"),
                _round(avg("radiation_usvh"),    4).alias("avg_radiation"),
                _round(_max("radiation_usvh"),   4).alias("max_radiation"),
                _round(avg("power_mwe"),         2).alias("avg_power_mwe"),
                count("reactor_id").alias("events_count"),
            )
        )

        # Strumień 1: każde zdarzenie -> POST /score
        query_events = (
            parsed.writeStream
            .outputMode("append")
            .foreachBatch(send_to_api)
            .option("checkpointLocation", "/tmp/checkpoint_events")
            .start()
        )

        # Strumień 2: agregaty okienne -> konsola
        query_windows = (
            windowed.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", False)
            .option("checkpointLocation", "/tmp/checkpoint_windows")
            .start()
        )

        print(f"Strumien uruchomiony — wysylam do Flask API + okna {WINDOW_DURATION} na konsoli.")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["stream", "api"], default="stream")
    args = parser.parse_args()
    main(args.mode)
