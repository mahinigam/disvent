FROM python:3.11-slim

WORKDIR /app

COPY apps/mock-generator/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/mock-generator/main.py ./
COPY schemas /schemas

ENV SCHEMA_PATH=/schemas/transaction.avsc

CMD ["python", "main.py"]
