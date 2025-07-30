# news_ingestor/Dockerfile (with JSON Query File)
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# NEW: Copy the queries.json file
COPY queries.json .

# Copy the rest of your application code
COPY . .

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--worker-class", "uvicorn.workers.UvicornWorker", "main:app"]
