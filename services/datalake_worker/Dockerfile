FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN adduser --disabled-password --gecos "" workeruser && chown -R workeruser:workeruser /app
USER workeruser

# Le worker n'expose aucun port HTTP, il lance directement sa boucle d'écoute Kafka
CMD ["python", "worker.py"]