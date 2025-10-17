# Use a slim Python image for smaller container size
FROM python:3.9-slim-buster

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY data_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY data_generator/data_generator.py .

# Command to run the application when the container starts
CMD ["python", "data_generator.py"]