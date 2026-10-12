FROM python:3.12-slim-bookworm
WORKDIR /app
COPY collector.py protocol.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "collector.py"]
