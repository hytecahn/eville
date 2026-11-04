FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir paho-mqtt==2.1.0
WORKDIR /app
COPY collector.py protocol.py archive.py reports.py maintenance.py remote_api.py daily.py packet_labels.py storage.py packet_ranges.py ha_operations.py moes_quality.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "collector.py"]
