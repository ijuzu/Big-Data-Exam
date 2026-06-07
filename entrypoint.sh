#!/bin/bash

set -euo pipefail

# Discover where pip installed pyspark (works for any Python version)
SPARK_HOME=$(python3 -c "
import pyspark, os
print(os.path.dirname(pyspark.__file__))
")

export SPARK_HOME
export PATH="${SPARK_HOME}/bin:${PATH}"
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

echo "SPARK_HOME = ${SPARK_HOME}"
echo "Java version: $(java -version 2>&1 | head -1)"
echo ""

# If no arguments were provided to 'docker run', use the default spark-submit command
if [ "$#" -eq 0 ]; then
    exec spark-submit \
        --master "local[*]" \
        --driver-memory 4g \
        --conf "spark.sql.shuffle.partitions=50" \
        --conf "spark.driver.maxResultSize=2g" \
        --conf "spark.sql.adaptive.enabled=true" \
        /app/src/main.py
else
    # Pass through any custom command (e.g. a different data path)
    exec "$@"
fi
