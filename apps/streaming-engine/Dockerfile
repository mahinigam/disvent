FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV SCHEMA_PATH=/schemas/transaction.avsc

COPY apps/streaming-engine/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/streaming-engine/main.py ./
COPY schemas /schemas

CMD ["python", "main.py"]
