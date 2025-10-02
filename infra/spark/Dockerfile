FROM openjdk:17-jdk-slim AS jdk
FROM python:3.11-slim

WORKDIR /app

# Copy Java into the Python base
COPY --from=jdk /usr/local/openjdk-17 /usr/local/openjdk-17

ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    procps \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utils/run-silver-pipeline.sh utils/
COPY utils/spark_silver_pipeline.py utils/
COPY data_generator/alt_generator.py data_generator/
COPY utils/otel_setup.py utils/

ENTRYPOINT [ "bash", "utils/run-silver-pipeline.sh" ]
