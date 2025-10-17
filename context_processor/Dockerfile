FROM python:3.9-slim-buster
WORKDIR /app
COPY context_processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY context_processor/context_processor.py .
# Copy the schemas directory into the container for validation
COPY schemas /app/schemas
CMD ["python", "context_processor.py"]