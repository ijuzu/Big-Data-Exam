FROM python:3.11-slim

# ── System dependencies ───────────────────────────────────────────────────────
# procps  → provides 'ps', used internally by Spark health checks
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# ── Java environment ──────────────────────────────────────────────────────────
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Python environment ────────────────────────────────────────────────────────
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# ── Application setup ─────────────────────────────────────────────────────────
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entrypoint script before the rest of the source so it is cached
# separately – rebuilds stay fast if only source code changes.
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY . /app

# Output directory for results.txt and collision_trajectory.png
RUN mkdir -p /app/output

# ── Runtime ───────────────────────────────────────────────────────────────────
# entrypoint.sh discovers SPARK_HOME at runtime and calls spark-submit.
# Override CMD to pass a custom data-path argument:
#   docker run <image> spark-submit --master local[*] --driver-memory 4g \
#       src/main.py "/app/data/raw/extracted/aisdk-2021-12-01.csv"
ENTRYPOINT ["/entrypoint.sh"]
CMD []
