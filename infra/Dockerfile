FROM openjdk:17-jdk-slim AS jdk
FROM python:3.11-slim

WORKDIR /app

# Java
COPY --from=jdk /usr/local/openjdk-17 /usr/local/openjdk-17
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev procps curl unzip \
    && rm -rf /var/lib/apt/lists/*


# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Project files
COPY data_generator/ data_generator/
COPY utils/ utils/
COPY utils/run-silver-pipeline.sh .
COPY utils/entrypoint.sh .

ENTRYPOINT [ "./entrypoint.sh" ]
