FROM python:3.9-slim-buster
WORKDIR /app
COPY ./predictive_maintenance_agent/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./predictive_maintenance_agent/predictive_maintenance_agent.py .
# Copy the model and features files
COPY ./predictive_maintenance_agent/model.pkl .
COPY ./predictive_maintenance_agent/features.json .
# Copy the schemas directory from the project root into the container
COPY ./schemas /app/schemas
CMD ["python", "predictive_maintenance_agent.py"]