# MaritimeIQ ops console + Spark local[*]
# Build: docker build -t maritimeiq .
# Run:   docker run --rm -p 8501:8501 --env-file .env -v "$PWD/data:/app/data:ro" -v "$PWD/lakehouse:/app/lakehouse" maritimeiq

FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.10 python3.10-venv python3-pip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.10 /usr/local/bin/python3

WORKDIR /app

COPY requirements.txt pyproject.toml Makefile ./
COPY src ./src
COPY configs ./configs
COPY scripts ./scripts
COPY data/hackathon ./data/hackathon

RUN python3 -m pip install --no-cache-dir -U pip setuptools wheel \
    && python3 -m pip install --no-cache-dir -r requirements.txt \
    && python3 -m pip install --no-cache-dir -e .

ENV MARITIMEIQ_PYTHON=/usr/bin/python3.10
ENV PYSPARK_PYTHON=/usr/bin/python3.10
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
ENV PYTHONPATH=/app/src
ENV JAVA_HOME=/opt/java/openjdk

EXPOSE 8501

# Prebuilt lakehouse recommended for Cloud Run; otherwise first UI click builds it.
CMD ["python3", "-m", "streamlit", "run", "src/maritimeiq/ui/app.py", "--server.port=8501", "--server.address=0.0.0.0", "--browser.gatherUsageStats=false"]
