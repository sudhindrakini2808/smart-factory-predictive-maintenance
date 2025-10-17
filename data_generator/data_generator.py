'''This script simulates multiple machines, generating sensor data and occasionally injecting "anomalies" (high temperature/vibration) 
that our AI agent will later try to detect.'''

import paho.mqtt.client as mqtt
import time
import json
import random
from datetime import datetime
import logging

# Configure logging for better visibility in Docker logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MQTT Broker settings
MQTT_BROKER_HOST = "mqtt_broker" # Service name in docker-compose
MQTT_BROKER_PORT = 1883
RAW_DATA_TOPIC_PREFIX = "raw_data/machine_sensors/"

# Simulated machines
MACHINE_IDS = ["CNC001", "ROBOT001", "INSPECTION001"]

# Initial sensor states (will be updated)
machine_states = {
    mid: {
        "temperature_c": random.uniform(20, 30),
        "vibration_g": random.uniform(0.1, 0.5),
        "power_kw": random.uniform(5, 15),
        "status": "running"
    } for mid in MACHINE_IDS
}

# Anomaly probability (e.g., 5% chance of an anomaly starting)
ANOMALY_PROBABILITY = 0.05
# How long an anomaly lasts (in cycles)
ANOMALY_DURATION_CYCLES = 5

# Keep track of active anomalies
active_anomalies = {mid: 0 for mid in MACHINE_IDS}

def generate_sensor_data(machine_id):
    """Generates simulated sensor data for a given machine."""
    state = machine_states[machine_id]

    # Simulate normal fluctuations
    state["temperature_c"] += random.uniform(-0.5, 0.5)
    state["vibration_g"] += random.uniform(-0.05, 0.05)
    state["power_kw"] += random.uniform(-0.2, 0.2)

    # Keep values within reasonable bounds
    state["temperature_c"] = max(20, min(state["temperature_c"], 60))
    state["vibration_g"] = max(0.1, min(state["vibration_g"], 2.0))
    state["power_kw"] = max(5, min(state["power_kw"], 30))

    # Introduce anomalies
    if active_anomalies[machine_id] > 0:
        # Anomaly is active, increase values significantly
        state["temperature_c"] += random.uniform(5, 10) # High temp
        state["vibration_g"] += random.uniform(0.5, 1.5) # High vib
        state["power_kw"] += random.uniform(2, 5) # High power
        state["status"] = random.choice(["running", "error"]) # Might show error
        active_anomalies[machine_id] -= 1
        logger.warning(f"Machine {machine_id}: ANOMALY ACTIVE! Remaining cycles: {active_anomalies[machine_id]}")
    elif random.random() < ANOMALY_PROBABILITY:
        # Start a new anomaly
        active_anomalies[machine_id] = ANOMALY_DURATION_CYCLES
        logger.error(f"Machine {machine_id}: ANOMALY STARTED!")
    else:
        state["status"] = "running" # Reset status if anomaly ends

    # Ensure anomaly values stay high
    if state["temperature_c"] < 60 and active_anomalies[machine_id] > 0: state["temperature_c"] = random.uniform(60, 90)
    if state["vibration_g"] < 2.0 and active_anomalies[machine_id] > 0: state["vibration_g"] = random.uniform(2.0, 4.0)
    if state["power_kw"] < 30 and active_anomalies[machine_id] > 0: state["power_kw"] = random.uniform(30, 45)


    data = {
        "machine_id": machine_id,
        "timestamp": datetime.now().isoformat(),
        "temperature_c": round(state["temperature_c"], 2),
        "vibration_g": round(state["vibration_g"], 2),
        "power_kw": round(state["power_kw"], 2),
        "status": state["status"]
    }
    return data

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
    else:
        logger.error(f"Failed to connect, return code {rc}\n")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    client.loop_start() # Start a non-blocking loop for network traffic

    logger.info("Data Generator started. Publishing simulated sensor data...")

    while True:
        for machine_id in MACHINE_IDS:
            sensor_data = generate_sensor_data(machine_id)
            topic = f"{RAW_DATA_TOPIC_PREFIX}{machine_id}"
            try:
                client.publish(topic, json.dumps(sensor_data))
                logger.info(f"Published to {topic}: {json.dumps(sensor_data)}")
            except Exception as e:
                logger.error(f"Error publishing to MQTT: {e}")
        time.sleep(random.uniform(1, 3)) # Simulate data every 1-3 seconds per machine

if __name__ == "__main__":
    main()