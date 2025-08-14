# syntax=docker/dockerfile:1
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential libssl-dev libffi-dev curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# app code
COPY ingest_live_data.py ./ingest_live_data.py

# directory for Tesla Oauth
VOLUME ["/cache"]

# env defaults
ENV TESLA_CACHE=/cache/tesla_token.json \
    INTERVAL_SECONDS=60 \
    PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=5s \
  CMD pgrep -f ingest_live_data.py || exit 1

CMD ["python", "ingest_live_data.py"]
