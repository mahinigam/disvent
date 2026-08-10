FROM python:3.11-slim

WORKDIR /app

COPY apps/dashboard/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/dashboard/main.py ./

EXPOSE 8501
CMD ["streamlit", "run", "main.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
