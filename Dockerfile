FROM python:3.11-slim

# Install rclone
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && curl -O https://downloads.rclone.org/rclone-current-linux-amd64.zip \
    && unzip rclone-current-linux-amd64.zip \
    && cp rclone-*-linux-amd64/rclone /usr/bin/ \
    && chmod 755 /usr/bin/rclone \
    && rm -rf rclone-* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs

# Make scripts executable
RUN chmod +x scripts/*.sh scripts/*.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.app"]

