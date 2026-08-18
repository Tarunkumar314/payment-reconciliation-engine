FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (separate layer for caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Default environment (overridden per-service in docker-compose)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# No default CMD — each service in docker-compose overrides with its own command
