import pytest
import json
import jsonschema
from datetime import datetime
import uuid
import os

# Base path for schemas
SCHEMA_DIR = "schemas"

def load_schema(filename):
    with open(os.path.join(SCHEMA_DIR, filename), 'r') as f:
        return json.load(f)

# Load all schemas once
RAW_MACHINE_DATA_SCHEMA = load_schema("raw_machine_data_v1.0.0.json")
PROCESSED_MACHINE_CONTEXT_SCHEMA = load_schema("processed_machine_context_v1.0.0.json")
MAINTENANCE_DECISION_SCHEMA = load_schema("maintenance_decision_v1.0.0.json")
ACTION_CONFIRMATION_SCHEMA = load_schema("action_confirmation_v1.0.0.json")
AGENT_HEARTBEAT_SCHEMA = load_schema("agent_heartbeat_v1.0.0.json")


# --- Test Raw Machine Data Schema ---
def test_raw_machine_data_valid():
    valid_data = {
        "machine_id": "CNC001",
        "timestamp": datetime.now().isoformat(),
        "temperature_c": 35.5,
        "vibration_g": 0.8,
        "power_kw": 12.3,
        "status": "running"
    }
    jsonschema.validate(instance=valid_data, schema=RAW_MACHINE_DATA_SCHEMA) # Should not raise error

def test_raw_machine_data_invalid_status():
    invalid_data = {
        "machine_id": "CNC001",
        "timestamp": datetime.now().isoformat(),
        "temperature_c": 35.5,
        "vibration_g": 0.8,
        "power_kw": 12.3,
        "status": "broken" # Invalid enum value
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=invalid_data, schema=RAW_MACHINE_DATA_SCHEMA)

# --- Test Processed Machine Context Schema ---
def test_processed_machine_context_valid():
    valid_context = {
        "context_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source_agent_id": "context_modeling_engine_001",
        "context_type": "machine_status_context",
        "schema_version": "1.0.0",
        "payload": {
            "machine_id": "CNC001",
            "current_status": "running",
            "avg_temperature_c_5min": 45.2,
            "max_vibration_g_1min": 1.2,
            "power_consumption_avg_10min": 25.0,
            "is_anomaly_detected": False
        },
        "metadata": {
            "priority": "normal",
            "ttl_seconds": 60
        }
    }
    jsonschema.validate(instance=valid_context, schema=PROCESSED_MACHINE_CONTEXT_SCHEMA)

def test_processed_machine_context_invalid_version():
    invalid_context = {
        "context_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source_agent_id": "context_modeling_engine_001",
        "context_type": "machine_status_context",
        "schema_version": "1.0", # Invalid semantic version
        "payload": {
            "machine_id": "CNC001",
            "current_status": "running",
            "avg_temperature_c_5min": 45.2,
            "max_vibration_g_1min": 1.2,
            "power_consumption_avg_10min": 25.0,
            "is_anomaly_detected": False
        },
        "metadata": {
            "priority": "normal",
            "ttl_seconds": 60
        }
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=invalid_context, schema=PROCESSED_MACHINE_CONTEXT_SCHEMA)

# --- Test Maintenance Decision Schema ---
def test_maintenance_decision_valid():
    valid_decision = {
        "decision_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source_agent_id": "predictive_maintenance_agent_001",
        "decision_type": "maintenance_decision",
        "schema_version": "1.0.0",
        "payload": {
            "machine_id": "CNC001",
            "needs_maintenance": True,
            "prediction_confidence": 0.95,
            "predicted_features": {
                "avg_temperature_c_5min": 75.0,
                "max_vibration_g_1min": 3.5,
                "power_consumption_avg_10min": 38.0
            }
        },
        "metadata": {
            "priority": "high",
            "ttl_seconds": 30
        }
    }
    jsonschema.validate(instance=valid_decision, schema=MAINTENANCE_DECISION_SCHEMA)

# --- Test Action Confirmation Schema ---
def test_action_confirmation_valid():
    valid_confirmation = {
        "confirmation_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source_agent_id": "action_executor_001",
        "confirmation_type": "action_confirmation",
        "schema_version": "1.0.0",
        "payload": {
            "machine_id": "CNC001",
            "decision_id": str(uuid.uuid4()),
            "action_taken": "maintenance_initiated",
            "status": "success",
            "details": "Simulated maintenance initiation"
        },
        "metadata": {
            "priority": "normal",
            "ttl_seconds": 30
        }
    }
    jsonschema.validate(instance=valid_confirmation, schema=ACTION_CONFIRMATION_SCHEMA)

# --- Test Agent Heartbeat Schema ---
def test_agent_heartbeat_valid():
    valid_heartbeat = {
        "agent_id": "predictive_maintenance_agent_001",
        "timestamp": datetime.now().isoformat(),
        "capabilities": {
            "consumes_context_types": [
                {"type": "machine_status_context", "schema_version": "1.0.0"}
            ],
            "produces_decision_types": [
                {"type": "maintenance_decision", "schema_version": "1.0.0"}
            ]
        },
        "status": "online"
    }
    jsonschema.validate(instance=valid_heartbeat, schema=AGENT_HEARTBEAT_SCHEMA)