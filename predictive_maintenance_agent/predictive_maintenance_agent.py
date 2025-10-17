'''
This agent will:

1. Load the pre-trained model.pkl.
2. Subscribe to context/machine_status/# topics.
3. Validate incoming context messages.
4. Extract relevant features from the context payload.
5. Use the loaded model to make a prediction (needs_maintenance).
6. Publish a decision message to decision/maintenance/<machine_id>.
7. Implement a basic "heartbeat" for agent discovery (part of MCP handshakes).
'''

import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import logging
import joblib
import pandas as pd
import jsonschema
import os
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MQTT Broker settings
MQTT_BROKER_HOST = "mqtt_broker"
MQTT_BROKER_PORT = 1883
CONTEXT_TOPIC_PREFIX = "context/machine_status/"
DECISION_TOPIC_PREFIX = "decision/maintenance/"
AGENT_DISCOVERY_TOPIC = "agent/discovery/heartbeat"
AGENT_ID = "predictive_maintenance_agent_001"

# Model and schema paths
MODEL_PATH = "model.pkl"
FEATURES_PATH = "features.json"
CONTEXT_SCHEMA_PATH = "schemas/processed_machine_context_v1.0.0.json"
DECISION_SCHEMA_PATH = "schemas/maintenance_decision_v1.0.0.json" # We'll create this soon
SUPPORTED_CONTEXT_SCHEMA_VERSION = "1.0.0"
DECISION_SCHEMA_VERSION = "1.0.0"

# Load model and features
model = None
features = None
context_schema = None
decision_schema = None

def load_artifacts():
    global model, features, context_schema, decision_schema
    try:
        model = joblib.load(MODEL_PATH)
        logger.info(f"ML model loaded from {MODEL_PATH}")
    except FileNotFoundError:
        logger.error(f"Model file not found at {MODEL_PATH}. Did you run train_model.py?")
        exit(1)
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        exit(1)

    try:
        with open(FEATURES_PATH, 'r') as f:
            features = json.load(f)
        logger.info(f"Features loaded from {FEATURES_PATH}: {features}")
    except FileNotFoundError:
        logger.error(f"Features file not found at {FEATURES_PATH}. Did you run train_model.py?")
        exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding features JSON: {e}")
        exit(1)

    try:
        with open(CONTEXT_SCHEMA_PATH, 'r') as f:
            context_schema = json.load(f)
        with open(DECISION_SCHEMA_PATH, 'r') as f:
            decision_schema = json.load(f)
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

def publish_heartbeat(client):
    """Publishes agent's capabilities for discovery."""
    heartbeat_message = {
        "agent_id": AGENT_ID,
        "timestamp": datetime.now().isoformat(),
        "capabilities": {
            "consumes_context_types": [
                {"type": "machine_status_context", "schema_version": SUPPORTED_CONTEXT_SCHEMA_VERSION}
            ],
            "produces_decision_types": [
                {"type": "maintenance_decision", "schema_version": DECISION_SCHEMA_VERSION}
            ]
        },
        "status": "online"
    }
    client.publish(AGENT_DISCOVERY_TOPIC, json.dumps(heartbeat_message), qos=0, retain=True)
    logger.info(f"Published heartbeat: {json.dumps(heartbeat_message)}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
        client.subscribe(f"{CONTEXT_TOPIC_PREFIX}#")
        logger.info(f"Subscribed to topic: {CONTEXT_TOPIC_PREFIX}#")
        publish_heartbeat(client) # Publish heartbeat on connect
    else:
        logger.error(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    try:
        context_message = json.loads(msg.payload.decode())
        machine_id = context_message.get("payload", {}).get("machine_id")

        if not machine_id:
            logger.warning(f"Received context without machine_id: {context_message}")
            return

        # 1. Validate incoming context
        if not validate_message(context_message, context_schema):
            logger.warning(f"Invalid context received for {machine_id}. Skipping.")
            return

        # Check schema version compatibility (simple check for now)
        if context_message.get("schema_version") != SUPPORTED_CONTEXT_SCHEMA_VERSION:
            logger.warning(f"Context schema version mismatch for {machine_id}. Expected {SUPPORTED_CONTEXT_SCHEMA_VERSION}, got {context_message.get('schema_version')}. Skipping.")
            return

        # 2. Extract features for prediction
        context_payload = context_message['payload']
        input_data = pd.DataFrame([context_payload], columns=features)

        # 3. Make prediction
        prediction = model.predict(input_data)[0]
        needs_maintenance = bool(prediction)
        prediction_proba = model.predict_proba(input_data)[0].tolist() # Probability for each class

        logger.info(f"Machine {machine_id}: Predicted needs_maintenance={needs_maintenance} (Proba: {prediction_proba})")

        # 4. Construct decision message
        decision_payload = {
            "machine_id": machine_id,
            "needs_maintenance": needs_maintenance,
            "prediction_confidence": max(prediction_proba),
            "predicted_features": {k: context_payload[k] for k in features}
        }
        decision_message = {
            "decision_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "source_agent_id": AGENT_ID,
            "decision_type": "maintenance_decision",
            "schema_version": DECISION_SCHEMA_VERSION,
            "payload": decision_payload,
            "metadata": {
                "priority": "high" if needs_maintenance else "normal",
                "ttl_seconds": 30
            }
        }

        # 5. Validate decision message
        if not validate_message(decision_message, decision_schema):
            logger.error(f"Generated decision for {machine_id} failed schema validation. This is an internal error.")
            return

        # 6. Publish decision
        decision_topic = f"{DECISION_TOPIC_PREFIX}{machine_id}"
        client.publish(decision_topic, json.dumps(decision_message))
        logger.info(f"Published decision to {decision_topic}: {json.dumps(decision_message)}")

    except json.JSONDecodeError:
        logger.error(f"Could not decode JSON from message: {msg.payload}")
    except KeyError as e:
        logger.error(f"Missing expected key in context message: {e}. Message: {context_message}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)

def main():
    load_artifacts()

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()