import math
from collections import deque
from datetime import datetime, timezone
import numpy as np


class VehicleHealthAnalyzer:
    """
    Real-time Vehicle Telemetry & Predictive Maintenance Engine.
    Combines domain-specific automotive diagnostics (OBD-II / CAN-Bus thresholds)
    with rolling statistical anomaly detection (Z-score & degradation slope)
    to predict Remaining Useful Life (RUL), component failure probabilities,
    and actionable maintenance recommendations.
    """

    def __init__(self, window_size=30):
        self.window_size = window_size
        self.vehicle_windows = {}
        self.vehicle_stats = {}

    def _get_window(self, vehicle_id):
        if vehicle_id not in self.vehicle_windows:
            self.vehicle_windows[vehicle_id] = {
                "engine_temp": deque(maxlen=self.window_size),
                "rpm": deque(maxlen=self.window_size),
                "battery_voltage": deque(maxlen=self.window_size),
                "oil_pressure": deque(maxlen=self.window_size),
                "coolant_level": deque(maxlen=self.window_size),
                "brake_pad_wear": deque(maxlen=self.window_size),
                "vibration": deque(maxlen=self.window_size),
            }
        return self.vehicle_windows[vehicle_id]

    @staticmethod
    def _safe_float(payload, keys, default):
        for k in keys:
            if k in payload and payload[k] is not None:
                try:
                    return float(payload[k])
                except (ValueError, TypeError):
                    pass
        return float(default)

    @staticmethod
    def _safe_str(payload, keys, default):
        for k in keys:
            if k in payload and payload[k] is not None:
                return str(payload[k])
        return str(default)

    def normalize_telemetry(self, raw_msg):
        """
        Normalizes varying schemas from Condense Data Simulator (Vehicle Health / Fleet Tracking)
        or custom OBD-II/CAN payloads into a unified canonical structure.
        """
        data = raw_msg.get("data", raw_msg) if isinstance(raw_msg, dict) else {}

        vehicle_id = self._safe_str(
            data, ["vehicle_id", "vehicleId", "device_id", "vin", "id"], "VEH-CFG-K6C8-01"
        )
        timestamp = self._safe_str(
            data, ["timestamp", "event_time", "ts"], datetime.now(timezone.utc).isoformat()
        )

        engine_temp = self._safe_float(
            data, ["engine_temp", "engineTemperature", "coolant_temp", "temp"], 88.0
        )
        rpm = self._safe_float(data, ["rpm", "engine_rpm", "engineRpm"], 2100.0)
        speed = self._safe_float(data, ["speed", "vehicle_speed", "speedKmph"], 55.0)
        battery_voltage = self._safe_float(
            data, ["battery_voltage", "batteryVoltage", "voltage"], 13.8
        )
        oil_pressure = self._safe_float(
            data, ["oil_pressure", "oilPressure", "engine_oil_pressure"], 42.0
        )
        coolant_level = self._safe_float(
            data, ["coolant_level", "coolantLevel"], 85.0
        )
        fuel_level = self._safe_float(data, ["fuel_level", "fuelLevel"], 68.0)
        brake_pad_wear = self._safe_float(
            data, ["brake_pad_wear", "brakePadWear", "brake_wear_pct"], 25.0
        )
        vibration = self._safe_float(
            data, ["vibration", "engine_vibration", "vibration_rms"], 0.35
        )
        dtc_codes = data.get("dtc_codes", data.get("fault_codes", []))
        if isinstance(dtc_codes, str):
            dtc_codes = [c.strip() for c in dtc_codes.split(",") if c.strip()]
        elif not isinstance(dtc_codes, list):
            dtc_codes = []

        lat = self._safe_float(data, ["latitude", "lat"], 16.5062)
        lon = self._safe_float(data, ["longitude", "lon", "lng"], 80.6480)

        return {
            "vehicle_id": vehicle_id,
            "timestamp": timestamp,
            "engine_temp": round(engine_temp, 2),
            "rpm": round(rpm, 1),
            "speed": round(speed, 1),
            "battery_voltage": round(battery_voltage, 2),
            "oil_pressure": round(oil_pressure, 2),
            "coolant_level": round(coolant_level, 1),
            "fuel_level": round(fuel_level, 1),
            "brake_pad_wear": round(brake_pad_wear, 1),
            "vibration": round(vibration, 3),
            "dtc_codes": dtc_codes,
            "location": {"lat": round(lat, 5), "lon": round(lon, 5)},
        }

    def analyze(self, raw_msg):
        t = self.normalize_telemetry(raw_msg)
        vid = t["vehicle_id"]
        win = self._get_window(vid)

        # Compute Z-scores before appending current reading
        z_scores = {}
        for metric in ["engine_temp", "rpm", "battery_voltage", "oil_pressure", "vibration"]:
            hist = list(win[metric])
            if len(hist) >= 5:
                mean = float(np.mean(hist))
                std = float(np.std(hist)) + 1e-5
                z_scores[metric] = round((t[metric] - mean) / std, 2)
            else:
                z_scores[metric] = 0.0

        # Append readings to rolling window
        for metric in win:
            win[metric].append(t[metric])

        # Compute Thermal Rise Trend (deg C per reading)
        temp_hist = list(win["engine_temp"])
        temp_slope = 0.0
        if len(temp_hist) >= 5:
            x = np.arange(len(temp_hist))
            temp_slope = float(np.polyfit(x, temp_hist, 1)[0])

        # Component Health & Failure Risk Scoring (0 to 100 risk)
        anomalies = []
        recommendations = []
        risk_penalties = 0.0

        # 1. Cooling & Thermal Subsystem
        thermal_risk = 0
        if t["engine_temp"] >= 112.0:
            thermal_risk = 95
            anomalies.append({
                "subsystem": "Cooling System",
                "severity": "CRITICAL",
                "metric": "engine_temp",
                "value": f"{t['engine_temp']} °C",
                "message": "Severe Engine Overheating! Immediate engine shutdown advised to prevent head gasket failure."
            })
            recommendations.append("Stop vehicle safely, inspect radiator fan, thermostat, and coolant leaks immediately.")
        elif t["engine_temp"] >= 102.0 or (temp_slope > 0.6 and t["engine_temp"] > 95.0):
            thermal_risk = 65
            anomalies.append({
                "subsystem": "Cooling System",
                "severity": "WARNING",
                "metric": "engine_temp",
                "value": f"{t['engine_temp']} °C (slope +{round(temp_slope, 2)}/tick)",
                "message": "Rapid thermal climb detected; cooling efficiency degrading."
            })
            recommendations.append("Schedule coolant flush and thermostat valve inspection within 24 hours.")
        elif t["coolant_level"] < 40.0:
            thermal_risk = 55
            anomalies.append({
                "subsystem": "Cooling System",
                "severity": "WARNING",
                "metric": "coolant_level",
                "value": f"{t['coolant_level']}%",
                "message": "Low coolant reservoir level detected."
            })
            recommendations.append("Top up OEM-spec coolant and pressure-test cooling hoses.")
        risk_penalties += thermal_risk * 0.30

        # 2. Lubrication & Oil Pressure Subsystem
        oil_risk = 0
        if t["oil_pressure"] < 18.0 and t["rpm"] > 1000:
            oil_risk = 92
            anomalies.append({
                "subsystem": "Lubrication System",
                "severity": "CRITICAL",
                "metric": "oil_pressure",
                "value": f"{t['oil_pressure']} PSI @ {t['rpm']} RPM",
                "message": "Critically low oil pressure under load! High risk of bearing seizure."
            })
            recommendations.append("Check engine oil level, oil pump relief valve, and oil filter blockage immediately.")
        elif t["oil_pressure"] < 26.0:
            oil_risk = 50
            anomalies.append({
                "subsystem": "Lubrication System",
                "severity": "WARNING",
                "metric": "oil_pressure",
                "value": f"{t['oil_pressure']} PSI",
                "message": "Sub-optimal oil pressure trend observed."
            })
            recommendations.append("Inspect oil viscosity and schedule oil & filter service.")
        risk_penalties += oil_risk * 0.25

        # 3. Electrical & Alternator/Battery Subsystem
        electrical_risk = 0
        if t["battery_voltage"] < 11.6:
            electrical_risk = 88
            anomalies.append({
                "subsystem": "Electrical & Charging",
                "severity": "CRITICAL",
                "metric": "battery_voltage",
                "value": f"{t['battery_voltage']} V",
                "message": "Severe battery drain or alternator charging failure."
            })
            recommendations.append("Test alternator output regulator and battery cell health before next trip.")
        elif t["battery_voltage"] < 12.3 or t["battery_voltage"] > 15.1:
            electrical_risk = 48
            anomalies.append({
                "subsystem": "Electrical & Charging",
                "severity": "WARNING",
                "metric": "battery_voltage",
                "value": f"{t['battery_voltage']} V",
                "message": "Voltage regulation outside optimal 12.4V–14.8V band."
            })
            recommendations.append("Inspect battery terminals and alternator belt tension.")
        risk_penalties += electrical_risk * 0.15

        # 4. Mechanical Vibration & Drivetrain Subsystem
        drivetrain_risk = 0
        if t["vibration"] > 1.25 or abs(z_scores.get("vibration", 0.0)) > 3.2:
            drivetrain_risk = 80
            anomalies.append({
                "subsystem": "Engine / Drivetrain Mounts",
                "severity": "CRITICAL",
                "metric": "vibration",
                "value": f"{t['vibration']} g (Z={z_scores.get('vibration', 0.0)})",
                "message": "Abnormal high-frequency vibration spike indicating misfire or mount/bearing wear."
            })
            recommendations.append("Perform cylinder compression/injector balance check and inspect engine mounts.")
        elif t["vibration"] > 0.85:
            drivetrain_risk = 42
            anomalies.append({
                "subsystem": "Engine / Drivetrain Mounts",
                "severity": "WARNING",
                "metric": "vibration",
                "value": f"{t['vibration']} g",
                "message": "Elevated mechanical vibration above baseline."
            })
            recommendations.append("Inspect wheel alignment, driveshaft CV joints, and engine mount bushings.")
        risk_penalties += drivetrain_risk * 0.15

        # 5. Brake Wear & Active DTC Codes
        brake_risk = 0
        if t["brake_pad_wear"] >= 85.0:
            brake_risk = 85
            anomalies.append({
                "subsystem": "Braking System",
                "severity": "CRITICAL",
                "metric": "brake_pad_wear",
                "value": f"{t['brake_pad_wear']}% worn",
                "message": "Brake pad lining near minimum safety thickness!"
            })
            recommendations.append("Replace front/rear brake pads and inspect rotor disc thickness.")
        elif t["brake_pad_wear"] >= 70.0:
            brake_risk = 45
            anomalies.append({
                "subsystem": "Braking System",
                "severity": "WARNING",
                "metric": "brake_pad_wear",
                "value": f"{t['brake_pad_wear']}% worn",
                "message": "Brake pads approaching replacement threshold."
            })
            recommendations.append("Order replacement brake pads for next scheduled pitstop.")
        risk_penalties += brake_risk * 0.15

        if t["dtc_codes"]:
            risk_penalties += min(25.0, len(t["dtc_codes"]) * 12.0)
            anomalies.append({
                "subsystem": "ECU Diagnostics (OBD-II)",
                "severity": "WARNING" if len(t["dtc_codes"]) == 1 else "CRITICAL",
                "metric": "dtc_codes",
                "value": ", ".join(t["dtc_codes"]),
                "message": f"Active ECU Diagnostic Trouble Codes reported: {', '.join(t['dtc_codes'])}"
            })
            recommendations.append(f"Run full OBD-II freeze-frame scan for DTC(s): {', '.join(t['dtc_codes'])}.")

        health_score = max(0.0, min(100.0, round(100.0 - risk_penalties, 1)))

        if health_score >= 80.0 and not any(a["severity"] == "CRITICAL" for a in anomalies):
            status = "HEALTHY"
        elif health_score >= 55.0 and not any(a["severity"] == "CRITICAL" for a in anomalies):
            status = "DEGRADING"
        else:
            status = "CRITICAL"

        # Predictive Remaining Useful Life (RUL in estimated operating hours before service)
        rul_hours = int(max(2, round((health_score / 100.0) ** 1.6 * 450)))
        if status == "CRITICAL":
            rul_hours = min(rul_hours, 12)

        if not recommendations:
            recommendations.append("All subsystems operating within nominal OEM parameters. Continue routine monitoring.")

        result = {
            "team_id": "CFG-K6C8",
            "vehicle_id": vid,
            "timestamp": t["timestamp"],
            "health_score": health_score,
            "status": status,
            "predicted_rul_hours": rul_hours,
            "telemetry": t,
            "subsystem_risks": {
                "cooling": thermal_risk,
                "lubrication": oil_risk,
                "electrical": electrical_risk,
                "drivetrain": drivetrain_risk,
                "braking": brake_risk,
            },
            "z_scores": z_scores,
            "anomalies": anomalies,
            "recommendations": list(dict.fromkeys(recommendations)),
        }
        self.vehicle_stats[vid] = result
        return result
