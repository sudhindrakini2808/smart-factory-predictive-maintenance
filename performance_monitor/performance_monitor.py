'''
This service will:

1. Subscribe to simulated_actions/# and context/# topics.
2. Log the details of actions taken and the context that led to them.
3. For simplicity, it will just print to console and could write to a file (though we'll only show console logging here).
'''

import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import logging
import jsonschema
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MQTT Broker settings
MQTT_BROKER_HOST = "mqtt_broker"
MQTT_BROKER_PORT = 1883
ACTION_CONFIRMATION_TOPIC_PREFIX = "simulated_actions/"
CONTEXT_TOPIC_PREFIX = "context/machine_status/"
AGENT_DISCOVERY_TOPIC = "agent/discovery/heartbeat" # Monitor heartbeats too
AGENT_ID = "performance_monitor_001"

# Schema paths for validation
ACTION_CONFIRMATION_SCHEMA_PATH = "schemas/action_confirmation_v1.0.0.json"
CONTEXT_SCHEMA_PATH = "schemas/processed_machine_context_v1.0.0.json"
AGENT_HEARTBEAT_SCHEMA_PATH = "schemas/agent_heartbeat_v1.0.0.json" # We'll create this soon

action_confirmation_schema = None
context_schema = None
agent_heartbeat_schema = None

# Simple in-memory store for recent contexts to link with actions
recent_contexts = {} # {machine_id: {context_id: context_message}}

def load_schemas():
    global action_confirmation_schema, context_schema, agent_heartbeat_schema
    try:
        with open(ACTION_CONFIRMATION_SCHEMA_PATH, 'r') as f:
            action_confirmation_schema = json.load(f)
        with open(CONTEXT_SCHEMA_PATH, 'r') as f:
            context_schema = json.load(f)
        with open(AGENT_HEARTBEAT_SCHEMA_PATH, 'r') as f:
            agent_heartbeat_schema = json.load(f)
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
        client.subscribe(f"{ACTION_CONFIRMATION_TOPIC_PREFIX}#")
        client.subscribe(f"{CONTEXT_TOPIC_PREFIX}#")
        client.subscribe(AGENT_DISCOVERY_TOPIC) # Subscribe to agent heartbeats
        logger.info(f"Subscribed to topics: {ACTION_CONFIRMATION_TOPIC_PREFIX}#, {CONTEXT_TOPIC_PREFIX}#, {AGENT_DISCOVERY_TOPIC}")
    else:
        logger.error(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    try:
        message = json.loads(msg.payload.decode())

        if msg.topic.startswith(ACTION_CONFIRMATION_TOPIC_PREFIX):
            if not validate_message(message, action_confirmation_schema):
                logger.warning(f"Invalid action confirmation received. Skipping.")
                return
            machine_id = message["payload"]["machine_id"]
            decision_id = message["payload"]["decision_id"]
            action_taken = message["payload"]["action_taken"]
            status = message["payload"]["status"]
            logger.info(f"PERFORMANCE MONITOR: Action Confirmed for {machine_id}: {action_taken} (Decision ID: {decision_id}, Status: {status})")
            # Here, you would typically store this in a database for later analysis
            # For now, we'll just log it.

        elif msg.topic.startswith(CONTEXT_TOPIC_PREFIX):
            if not validate_message(message, context_schema):
                logger.warning(f"Invalid context received. Skipping.")
                return
            machine_id = message["payload"]["machine_id"]
            context_id = message["context_id"]
            # Store recent context for potential linking with actions
            if machine_id not in recent_contexts:
                recent_contexts[machine_id] = {}
            recent_contexts[machine_id][context_id] = message
            # Clean up old contexts (e.g., older than 5 minutes)
            for cid, ctx in list(recent_contexts[machine_id].items()):
                if datetime.fromisoformat(ctx['timestamp']) < datetime.now() - timedelta(minutes=5):
                    del recent_contexts[machine_id][cid]
            logger.debug(f"PERFORMANCE MONITOR: Received context for {machine_id}. Stored {len(recent_contexts[machine_id])} contexts.")

        elif msg.topic == AGENT_DISCOVERY_TOPIC:
            if not validate_message(message, agent_heartbeat_schema):
                logger.warning(f"Invalid agent heartbeat received. Skipping.")
                return
            agent_id = message.get("agent_id")
            status = message.get("status")
            capabilities = message.get("capabilities")
            logger.info(f"PERFORMANCE MONITOR: Agent Heartbeat from {agent_id} - Status: {status}, Capabilities: {capabilities}")
            # This information could be used to build a topology of active agents

    except json.JSONDecodeError:
        logger.error(f"Could not decode JSON from message: {msg.payload}")
    except KeyError as e:
        logger.error(f"Missing expected key in message: {e}. Message: {message}")
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