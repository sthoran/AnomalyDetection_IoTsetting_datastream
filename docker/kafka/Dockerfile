# Use a lightweight Python base image
FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy the application files
COPY requirements.txt requirements.txt
COPY kafka_producer.py kafka_producer.py

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the Kafka producer
CMD ["python", "kafka_producer.py"]
