import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib
import random
import os
import json

# Define the output directory for the model
MODEL_DIR = "predictive_maintenance_agent"
MODEL_PATH = os.path.join(MODEL_DIR, "model.pkl")

# Ensure the model directory exists
os.makedirs(MODEL_DIR, exist_ok=True)

print("Generating synthetic training data...")

data = []
for _ in range(1000): # Generate 1000 data points
    machine_id = random.choice(["CNC001", "ROBOT001", "INSPECTION001"])
    
    # Simulate normal operating conditions
    avg_temperature_c_5min = random.uniform(25, 55)
    max_vibration_g_1min = random.uniform(0.1, 1.5)
    power_consumption_avg_10min = random.uniform(10, 25)
    
    # Introduce scenarios where maintenance is likely needed
    needs_maintenance = 0 # Default to no maintenance

    # High temperature or high vibration indicates maintenance needed
    if avg_temperature_c_5min > 60 and random.random() < 0.8: # 80% chance if temp is high
        needs_maintenance = 1
        avg_temperature_c_5min = random.uniform(60, 95) # Make temp actually high
        max_vibration_g_1min = random.uniform(1.5, 4.0) # Might also have higher vib
        power_consumption_avg_10min = random.uniform(25, 45) # Might also have higher power
    elif max_vibration_g_1min > 2.0 and random.random() < 0.8: # 80% chance if vib is high
        needs_maintenance = 1
        max_vibration_g_1min = random.uniform(2.0, 4.5) # Make vib actually high
        avg_temperature_c_5min = random.uniform(40, 70) # Temp might also be elevated
        power_consumption_avg_10min = random.uniform(20, 40) # Power might also be elevated
    elif power_consumption_avg_10min > 35 and random.random() < 0.6: # 60% chance if power is high
        needs_maintenance = 1
        power_consumption_avg_10min = random.uniform(35, 48) # Make power actually high
        avg_temperature_c_5min = random.uniform(50, 80) # Temp might also be elevated
        max_vibration_g_1min = random.uniform(1.0, 3.0) # Vib might also be elevated

    # Add some noise to the 'needs_maintenance' label to make it less perfect
    if random.random() < 0.05: # 5% chance to flip the label
        needs_maintenance = 1 - needs_maintenance

    data.append([
        machine_id,
        avg_temperature_c_5min,
        max_vibration_g_1min,
        power_consumption_avg_10min,
        needs_maintenance
    ])

df = pd.DataFrame(data, columns=[
    'machine_id',
    'avg_temperature_c_5min',
    'max_vibration_g_1min',
    'power_consumption_avg_10min',
    'needs_maintenance'
])

print(f"Generated {len(df)} data points. Sample:\n{df.head()}")
print(f"Maintenance needed distribution:\n{df['needs_maintenance'].value_counts()}")

# Features and target
features = ['avg_temperature_c_5min', 'max_vibration_g_1min', 'power_consumption_avg_10min']
X = df[features]
y = df['needs_maintenance']

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a simple Decision Tree Classifier
print("Training Decision Tree Classifier...")
model = DecisionTreeClassifier(random_state=42)
model.fit(X_train, y_train)

# Evaluate the model
y_pred = model.predict(X_test)
print(f"\nModel Accuracy: {accuracy_score(y_test, y_pred):.2f}")
print("Classification Report:\n", classification_report(y_test, y_pred))

# Save the trained model
joblib.dump(model, MODEL_PATH)
print(f"Model saved to {MODEL_PATH}")

# Save features for agent to know the order
with open(os.path.join(MODEL_DIR, "features.json"), 'w') as f:
    json.dump(features, f)
print(f"Features list saved to {os.path.join(MODEL_DIR, 'features.json')}")

print("\nModel training complete. You can now proceed with the predictive_maintenance_agent setup.")