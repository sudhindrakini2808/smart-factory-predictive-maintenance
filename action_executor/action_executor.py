'''
This service will:

1. Subscribe to decision/# topics.
2. Validate incoming decision messages (using the maintenance_decision_v1.0.0.json schema).
3. Simulate the execution of the action (e.g., print a message to the console).
4. Publish a confirmation of the simulated action.
'''

import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import logging
import jsonschema
import os
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MQTT Broker settings
MQTT_BROKER_HOST = "mqtt_broker"
MQTT_BROKER_PORT = 1883
DECISION_TOPIC_PREFIX = "decision/"
SIMULATED_ACTION_TOPIC_PREFIX = "simulated_actions/"
AGENT_ID = "action_executor_001"

# Schema paths
DECISION_SCHEMA_PATH = "schemas/maintenance_decision_v1.0.0.json"
ACTION_CONFIRMATION_SCHEMA_PATH = "schemas/action_confirmation_v1.0.0.json" # We'll create this soon
SUPPORTED_DECISION_SCHEMA_VERSION = "1.0.0"
ACTION_CONFIRMATION_SCHEMA_VERSION = "1.0.0"


decision_schema = None
action_confirmation_schema = None

def load_schemas():
    global decision_schema, action_confirmation_schema
    try:
        with open(DECISION_SCHEMA_PATH, 'r') as f:
            decision_schema = json.load(f)
        with open(ACTION_CONFIRMATION_SCHEMA_PATH, 'r') as f:
            action_confirmation_schema = json.load(f)
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

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
        # Subscribe to all decision topics
        client.subscribe(f"{DECISION_TOPIC_PREFIX}#")
        logger.info(f"Subscribed to topic: {DECISION_TOPIC_PREFIX}#")
    else:
        logger.error(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    try:
        decision_message = json.loads(msg.payload.decode())
        decision_type = decision_message.get("decision_type")
        machine_id = decision_message.get("payload", {}).get("machine_id")

        if not machine_id or not decision_type:
            logger.warning(f"Received decision without machine_id or decision_type: {decision_message}")
            return

        # 1. Validate incoming decision
        if not validate_message(decision_message, decision_schema):
            logger.warning(f"Invalid decision received for {machine_id}. Skipping.")
            return

        # Check schema version compatibility
        if decision_message.get("schema_version") != SUPPORTED_DECISION_SCHEMA_VERSION:
            logger.warning(f"Decision schema version mismatch for {machine_id}. Expected {SUPPORTED_DECISION_SCHEMA_VERSION}, got {decision_message.get('schema_version')}. Skipping.")
            return

        # Simulate action based on decision type
        if decision_type == "maintenance_decision":
            needs_maintenance = decision_message["payload"]["needs_maintenance"]
            if needs_maintenance:
                logger.critical(f"SIMULATING ACTION: Initiating maintenance for {machine_id} based on AI decision!")
                # In a real system, this would trigger a PLC command, work order, etc.
            else:
                logger.info(f"SIMULATING ACTION: No maintenance needed for {machine_id}. Continuing normal operation.")
        else:
            logger.warning(f"Unknown decision type received: {decision_type} for {machine_id}. Skipping action.")
            return

        # 2. Publish action confirmation
        confirmation_payload = {
            "machine_id": machine_id,
            "decision_id": decision_message["decision_id"],
            "action_taken": "maintenance_initiated" if needs_maintenance else "no_action_taken",
            "status": "success",
            "details": f"Simulated {decision_type} action for {machine_id}"
        }
        action_confirmation_message = {
            "confirmation_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "source_agent_id": AGENT_ID,
            "confirmation_type": "action_confirmation",
            "schema_version": ACTION_CONFIRMATION_SCHEMA_VERSION,
            "payload": confirmation_payload,
            "metadata": {
                "priority": "normal",
                "ttl_seconds": 30
            }
        }

        # 3. Validate action confirmation message
        if not validate_message(action_confirmation_message, action_confirmation_schema):
            logger.error(f"Generated action confirmation for {machine_id} failed schema validation. This is an internal error.")
            return

        confirmation_topic = f"{SIMULATED_ACTION_TOPIC_PREFIX}{machine_id}"
        client.publish(confirmation_topic, json.dumps(action_confirmation_message))
        logger.info(f"Published action confirmation to {confirmation_topic}: {json.dumps(action_confirmation_message)}")

    except json.JSONDecodeError:
        logger.error(f"Could not decode JSON from message: {msg.payload}")
    except KeyError as e:
        logger.error(f"Missing expected key in decision message: {e}. Message: {decision_message}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)

def main():
    load_schemas()

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()