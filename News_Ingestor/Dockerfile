# news_ingestor/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# Command to run FastAPI app with Uvicorn workers via Gunicorn
# 'main:app' refers to the 'app' FastAPI instance in main.py
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--worker-class", "uvicorn.workers.UvicornWorker", "main:app"]
