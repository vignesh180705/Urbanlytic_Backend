# Dockerfile (pointing to asgi_app)
# Use the official Python 3.10 slim image for a smaller footprint
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Cloud Run automatically sets the PORT environment variable.
# Your application must listen on this port.
EXPOSE 8080

# Define the command to run your application using Gunicorn with Uvicorn workers
# Point to 'main:asgi_app' which is the WSGI2ASGI-wrapped Flask application
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--worker-class", "uvicorn.workers.UvicornWorker", "main:asgi_app"]
