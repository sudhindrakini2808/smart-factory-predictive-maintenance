FROM python:3.9-slim-buster
WORKDIR /app
COPY ./action_executor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./action_executor/action_executor.py .
# Copy the schemas directory from the project root into the container
COPY ./schemas /app/schemas
CMD ["python", "action_executor.py"]