'''
This service will:

1. Subscribe to raw data topics.
2. Validate incoming messages against raw_machine_data_v1.0.0.json.
3. Maintain a small in-memory buffer of recent data for each machine.
4. Perform simple aggregations (average temperature, max vibration, average power).
4. Detect a basic anomaly if temperature or vibration exceeds a threshold.
5. Construct a new context message.
5. Validate the new context message against processed_machine_context_v1.0.0.json.
7. Publish the validated context message to a new topic.'''


import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime, timedelta
import logging
from collections import deque
import jsonschema
import os
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MQTT Broker settings
MQTT_BROKER_HOST = "mqtt_broker"
MQTT_BROKER_PORT = 1883
RAW_DATA_TOPIC_PREFIX = "raw_data/machine_sensors/"
PROCESSED_CONTEXT_TOPIC_PREFIX = "context/machine_status/"
AGENT_ID = "context_modeling_engine_001"

# Schema paths (relative to the container's /app directory)
RAW_SCHEMA_PATH = "schemas/raw_machine_data_v1.0.0.json"
CONTEXT_SCHEMA_PATH = "schemas/processed_machine_context_v1.0.0.json"
CONTEXT_SCHEMA_VERSION = "1.0.0"

# Data buffer for aggregation (e.g., last 10 minutes of data)
# Using deque for efficient appending and popping from both ends
DATA_BUFFERS = {} # {machine_id: deque(maxlen=60)} assuming 10 sec interval for 10 min

# Load JSON schemas
raw_schema = None
context_schema = None

def load_schemas():
    global raw_schema, context_schema
    try:
        with open(RAW_SCHEMA_PATH, 'r') as f:
            raw_schema = json.load(f)
        with open(CONTEXT_SCHEMA_PATH, 'r') as f:
            context_schema = json.load(f)
        logger.info("JSON schemas loaded successfully.")
    except FileNotFoundError as e:
        logger.error(f"Schema file not found: {e}. Make sure schemas are copied correctly.")
        exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON schema: {e}")
        exit(1)

def validate_message(message, schema):
    """Validates a message against a given JSON schema."""
    try:
        jsonschema.validate(instance=message, schema=schema)
        return True
    except jsonschema.ValidationError as e:
        logger.warning(f"Schema validation failed: {e.message} for message: {message}")
        return False

def process_raw_data(machine_id, data):
    """Aggregates raw data and generates context."""
    if machine_id not in DATA_BUFFERS:
        # Assuming data comes every 1-3 seconds, 60 entries covers 1-3 minutes
        # For 5-10 min aggregation, we need more. Let's aim for 10 min buffer (approx 300 entries at 2s interval)
        DATA_BUFFERS[machine_id] = deque(maxlen=300)

    DATA_BUFFERS[machine_id].append(data)

    current_time = datetime.now()

    # Filter data for specific time windows
    data_5min = [d for d in DATA_BUFFERS[machine_id] if datetime.fromisoformat(d['timestamp']) > current_time - timedelta(minutes=5)]
    data_1min = [d for d in DATA_BUFFERS[machine_id] if datetime.fromisoformat(d['timestamp']) > current_time - timedelta(minutes=1)]
    data_10min = [d for d in DATA_BUFFERS[machine_id] if datetime.fromisoformat(d['timestamp']) > current_time - timedelta(minutes=10)]


    avg_temperature_c_5min = sum(d['temperature_c'] for d in data_5min) / len(data_5min) if data_5min else 0
    max_vibration_g_1min = max(d['vibration_g'] for d in data_1min) if data_1min else 0
    power_consumption_avg_10min = sum(d['power_kw'] for d in data_10min) / len(data_10min) if data_10min else 0

    # Simple anomaly detection: high temp OR high vib
    is_anomaly_detected = (avg_temperature_c_5min > 70) or (max_vibration_g_1min > 3.0)

    # Get the most recent status
    current_status = data['status']

    context_payload = {
        "machine_id": machine_id,
        "current_status": current_status,
        "avg_temperature_c_5min": round(avg_temperature_c_5min, 2),
        "max_vibration_g_1min": round(max_vibration_g_1min, 2),
        "power_consumption_avg_10min": round(power_consumption_avg_10min, 2),
        "is_anomaly_detected": is_anomaly_detected
    }

    context_message = {
        "context_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source_agent_id": AGENT_ID,
        "context_type": "machine_status_context",
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "payload": context_payload,
        "metadata": {
            "priority": "high" if is_anomaly_detected else "normal",
            "ttl_seconds": 60 # Context valid for 60 seconds
        }
    }
    return context_message

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
        # Subscribe to all raw machine sensor data
        client.subscribe(f"{RAW_DATA_TOPIC_PREFIX}#")
        logger.info(f"Subscribed to topic: {RAW_DATA_TOPIC_PREFIX}#")
    else:
        logger.error(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    try:
        raw_data = json.loads(msg.payload.decode())
        machine_id = raw_data.get("machine_id")

        if not machine_id:
            logger.warning(f"Received message without machine_id: {raw_data}")
            return

        # 1. Validate raw data
        if not validate_message(raw_data, raw_schema):
            logger.warning(f"Invalid raw data received for {machine_id}. Skipping.")
            return

        # 2. Process and generate context
        context_message = process_raw_data(machine_id, raw_data)

        # 3. Validate generated context
        if not validate_message(context_message, context_schema):
            logger.error(f"Generated context for {machine_id} failed schema validation. This is an internal error.")
            return

        # 4. Publish validated context
        context_topic = f"{PROCESSED_CONTEXT_TOPIC_PREFIX}{machine_id}"
        client.publish(context_topic, json.dumps(context_message))
        logger.info(f"Published context to {context_topic}: {json.dumps(context_message)}")

    except json.JSONDecodeError:
        logger.error(f"Could not decode JSON from message: {msg.payload}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)

def main():
    load_schemas()

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    client.loop_forever() # Block and listen for messages

if __name__ == "__main__":
    main()