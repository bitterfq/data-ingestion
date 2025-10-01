FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_generator/alt_generator.py data_generator/
COPY utils/otel_setup.py utils/

ENTRYPOINT [ "python", "data_generator/alt_generator.py" ]