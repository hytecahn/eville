FROM python:3.12-slim-bookworm
WORKDIR /app
COPY collector.py protocol.py archive.py reports.py maintenance.py remote_api.py daily.py packet_labels.py storage.py packet_ranges.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "collector.py"]
