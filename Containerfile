FROM docker.io/library/python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    HUB_CONFIG=/config/config.yaml

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENTRYPOINT ["python", "-m", "app.main"]
