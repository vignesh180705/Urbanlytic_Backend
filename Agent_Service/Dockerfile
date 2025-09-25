# Dockerfile (Flask Version - Older)
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# Command to run Flask app with Uvicorn workers via Gunicorn
# 'main:asgi_app' refers to the WSGIMiddleware-wrapped Flask instance in main.py
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--worker-class", "uvicorn.workers.UvicornWorker", "main:asgi_app"]