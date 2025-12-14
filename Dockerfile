FROM jorineg/ibhelm-base:latest

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

RUN mkdir -p /app/data /app/logs

ENV PYTHONUNBUFFERED=1
ENV SERVICE_NAME=filemetadatasync

CMD ["python", "-m", "src.app"]
