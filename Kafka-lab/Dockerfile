FROM python:3.12-slim
# Install once at build time (with patience for a slow network), not on every pod start
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 confluent-kafka
ENV PYTHONUNBUFFERED=1
WORKDIR /app