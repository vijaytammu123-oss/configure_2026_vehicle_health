import json
import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template
from kafka import KafkaConsumer, KafkaProducer

from predictive_engine import VehicleHealthAnalyzer

load_dotenv()

app = Flask(__name__)

# Environment Variables (Configured in Condense UI or .env)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-sandbox.condense.io:9092")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "cfg_k6c8_vehicle_health_raw")
ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "cfg_k6c8_vehicle_health_alerts")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "cfg_k6c8_vehicle_health_group")
TEAM_ID = os.getenv("TEAM_ID", "CFG-K6C8")
PORT = int(os.getenv("DASHBOARD_PORT", "5000"))
SIMULATE_LOCAL = os.getenv("SIMULATE_LOCAL_IF_NO_KAFKA", "false").lower() == "true"

analyzer = VehicleHealthAnalyzer(window_size=30)
recent_events = deque(maxlen=100)
recent_alerts = deque(maxlen=50)
pipeline_metrics = {
    "messages_processed": 0,
    "alerts_emitted": 0,
    "kafka_connected": False,
    "mode": "INITIALIZING",
    "last_error": None,
    "started_at": datetime.now(timezone.utc).isoformat(),
}


def build_kafka_config():
    cfg = {
        "bootstrap_servers": [s.strip() for s in BOOTSTRAP_SERVERS.split(",") if s.strip()],
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        cfg["security_protocol"] = SECURITY_PROTOCOL
        cfg["sasl_mechanism"] = SASL_MECHANISM
        cfg["sasl_plain_username"] = KAFKA_USERNAME
        cfg["sasl_plain_password"] = KAFKA_PASSWORD
    return cfg


def create_producer():
    try:
        cfg = build_kafka_config()
        producer = KafkaProducer(
            **cfg,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        return producer
    except Exception as exc:
        pipeline_metrics["last_error"] = f"Producer init error: {exc}"
        return None


def process_payload(payload, producer=None):
    analysis = analyzer.analyze(payload)
    recent_events.appendleft(analysis)
    pipeline_metrics["messages_processed"] += 1

    if analysis["status"] in ("DEGRADING", "CRITICAL") or analysis["anomalies"]:
        alert_packet = {
            "alert_id": f"ALT-{int(time.time() * 1000)}-{analysis['vehicle_id']}",
            "team_id": TEAM_ID,
            "vehicle_id": analysis["vehicle_id"],
            "timestamp": analysis["timestamp"],
            "severity": analysis["status"],
            "health_score": analysis["health_score"],
            "predicted_rul_hours": analysis["predicted_rul_hours"],
            "anomalies": analysis["anomalies"],
            "recommendations": analysis["recommendations"],
        }
        recent_alerts.appendleft(alert_packet)
        pipeline_metrics["alerts_emitted"] += 1

        if producer is not None:
            try:
                producer.send(ALERT_TOPIC, value=alert_packet)
            except Exception as exc:
                pipeline_metrics["last_error"] = f"Alert publish error: {exc}"


def local_demo_simulator_loop():
    """Fallback local telemetry generator when testing offline before Kafka credentials arrive."""
    pipeline_metrics["mode"] = "LOCAL_SIMULATION"
    fleet = ["AP-16-VK-2010", "TS-09-VH-4512", "AP-07-MT-8890", "KA-01-CD-1024"]
    tick = 0
    while True:
        tick += 1
        for vid in fleet:
            # Inject realistic degradation cycle on one vehicle every ~15 ticks
            degrade = (vid == "AP-16-VK-2010" and (tick % 20) in range(10, 18))
            sample = {
                "vehicle_id": vid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "engine_temp": round(random.uniform(104.0, 115.5) if degrade else random.uniform(84.0, 94.0), 1),
                "rpm": random.randint(2800, 3600) if degrade else random.randint(1500, 2400),
                "speed": random.randint(65, 95),
                "battery_voltage": round(random.uniform(11.3, 12.1) if degrade else random.uniform(13.5, 14.3), 2),
                "oil_pressure": round(random.uniform(15.0, 23.0) if degrade else random.uniform(36.0, 48.0), 1),
                "coolant_level": round(random.uniform(35.0, 55.0) if degrade else random.uniform(78.0, 95.0), 1),
                "fuel_level": round(random.uniform(30.0, 90.0), 1),
                "brake_pad_wear": round(random.uniform(72.0, 88.0) if vid == "TS-09-VH-4512" else random.uniform(18.0, 45.0), 1),
                "vibration": round(random.uniform(1.1, 1.45) if degrade else random.uniform(0.22, 0.48), 3),
                "dtc_codes": ["P0217", "P0524"] if degrade else [],
            }
            process_payload(sample, producer=None)
        time.sleep(1.5)


def kafka_stream_worker():
    if SIMULATE_LOCAL or not KAFKA_USERNAME or not KAFKA_PASSWORD:
        local_demo_simulator_loop()
        return

    producer = create_producer()
    cfg = build_kafka_config()

    while True:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                **cfg,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            pipeline_metrics["kafka_connected"] = True
            pipeline_metrics["mode"] = "CONDENSE_KAFKA_LIVE"
            pipeline_metrics["last_error"] = None

            for message in consumer:
                if isinstance(message.value, dict):
                    process_payload(message.value, producer=producer)
        except Exception as exc:
            pipeline_metrics["kafka_connected"] = False
            pipeline_metrics["mode"] = "RECONNECTING"
            pipeline_metrics["last_error"] = str(exc)
            time.sleep(5)


@app.route("/")
def index():
    return render_template("index.html", team_id=TEAM_ID, input_topic=INPUT_TOPIC, alert_topic=ALERT_TOPIC)


@app.route("/api/state")
def api_state():
    vehicles = list(analyzer.vehicle_stats.values())
    vehicles.sort(key=lambda v: v["health_score"])
    avg_health = round(sum(v["health_score"] for v in vehicles) / len(vehicles), 1) if vehicles else 100.0
    critical_count = sum(1 for v in vehicles if v["status"] == "CRITICAL")
    degrading_count = sum(1 for v in vehicles if v["status"] == "DEGRADING")

    return jsonify({
        "team_id": TEAM_ID,
        "metrics": pipeline_metrics,
        "summary": {
            "total_vehicles": len(vehicles),
            "avg_health_score": avg_health,
            "critical_vehicles": critical_count,
            "degrading_vehicles": degrading_count,
        },
        "vehicles": vehicles,
        "recent_alerts": list(recent_alerts)[:20],
        "recent_events": list(recent_events)[:15],
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok", "team_id": TEAM_ID, "mode": pipeline_metrics["mode"]})


if __name__ == "__main__":
    worker_thread = threading.Thread(target=kafka_stream_worker, daemon=True)
    worker_thread.start()
    app.run(host="0.0.0.0", port=PORT, debug=False)
