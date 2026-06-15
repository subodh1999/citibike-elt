FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY station_info_ingest.py .
CMD ["python", "station_info_ingest.py"]