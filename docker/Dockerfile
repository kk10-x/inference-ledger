FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
# The chaos package ships in the image so the mock provider can run as a
# service under the chaos overlay. It is inert unless explicitly started.
COPY chaos/ /app/chaos/
ENV PYTHONPATH=/app/src:/app PYTHONUNBUFFERED=1

# The gateway traps SIGTERM to drain in-flight streams; do not let a shell
# swallow the signal. Both services are started via `command:` in compose.
STOPSIGNAL SIGTERM
