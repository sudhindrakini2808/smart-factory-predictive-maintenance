FROM python:3.9-slim-buster
WORKDIR /app
COPY ./performance_monitor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./performance_monitor/performance_monitor.py .
# Copy the schemas directory from the project root into the container
COPY ./schemas /app/schemas
CMD ["python", "performance_monitor.py"]