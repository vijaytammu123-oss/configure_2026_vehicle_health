# Configure 2026 | Mobility Hackathon — Predictive Vehicle Health Twin
**Team ID:** `CFG-K6C8` | **Participant:** Vijaya Krishna Tammu (`@vijaytammu123`)

## Architecture Overview
1. **Ingestion Layer (Condense Data Simulator / OBD-II Stream):** Streams real-time vehicle telemetry (`engine_temp`, `rpm`, `battery_voltage`, `oil_pressure`, `coolant_level`, `brake_pad_wear`, `vibration`, `dtc_codes`) into the prefixed Kafka input topic.
2. **Stream Processing & Predictive Engine (`predictive_engine.py`):**
   - Normalizes heterogeneous telemetry payloads in real time.
   - Computes rolling 30-sample **Z-scores** and **thermal rise slopes** (`deg C/tick`) alongside automotive domain thresholds.
   - Calculates a weighted **Vehicle Health Score (0–100%)**, subsystem risk breakdown, and **Predicted Remaining Useful Life (RUL in hours)**.
3. **Alerting & Digital Twin Dashboard (`app.py`):**
   - Publishes structured predictive maintenance alerts to `<prefix>vehicle_health_alerts` Kafka topic.
   - Serves a live command-center UI on port `5000` showing real-time fleet twins, RUL estimates, and actionable technician recommendations.

## Quick Setup in Condense Applications
1. Upload or clone this repository into **Condense -> Applications** (select **Python**).
2. Add the Environment Variables listed in `.env.example` using your Kafka Sandbox credentials and assigned **Topic / Consumer Group Prefix**.
3. Click **Publish Application** (Type: `Transform` or `Output`, Image: `predictive-vehicle-twin`, Version: `1.0`) and **Deploy**.
4. Connect your **Vehicle Health Data Simulator** connector to `<prefix>vehicle_health_raw` in the Condense Pipeline Canvas.
