FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1

# The gateway traps SIGTERM to drain in-flight streams; do not let a shell
# swallow the signal. Both services are started via `command:` in compose.
STOPSIGNAL SIGTERM
