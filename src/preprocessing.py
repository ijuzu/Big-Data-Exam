"""
preprocessing.py
────────────────
Optimized AIS preprocessing pipeline:
- reduced shuffle stages
- consistent time bucketing
- removal of redundant aggregation DAG
"""

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_KM  = 92.6


# ─────────────────────────────────────────────────────────────────────────────
def load_data(spark, path: str):
    df = spark.read.csv(path, header=True, inferSchema=True)

    rename_map = {
        c: c.strip().lstrip("#").strip().replace(" ", "_").lower()
        for c in df.columns
    }

    for old, new in rename_map.items():
        if old != new:
            df = df.withColumnRenamed(old, new)

    if "name" not in df.columns:
        df = df.withColumn("name", F.lit("UNKNOWN"))

    return df


# ─────────────────────────────────────────────────────────────────────────────
def filter_area_and_time(df):
    """
    3-stage spatial filter with early pruning.
    """

    df = df.withColumn("timestamp",
                       F.coalesce(
                           F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
                           F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
                           F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
                           F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS"),))
    

    df = df.withColumn("latitude", F.col("latitude").cast(DoubleType())) \
           .withColumn("longitude", F.col("longitude").cast(DoubleType())) \
           .withColumn("sog", F.col("sog").cast(DoubleType()))

    df = df.dropna(subset=["timestamp", "latitude", "longitude", "mmsi"])

    # ── fast filters first
    df = df.filter(
        (F.col("type_of_mobile").isin("Class A")) &
        (F.col("cog") != 511) & (F.col("cog").between(0, 360)) &
        (F.col("navigational_status").isin(
        "Under way using engine", "Under way sailing")) &
        ~(F.lower(F.col("name")).contains("rescue") | F.lower(F.col("name")).contains("sar")) &
        (F.col("mmsi") > 99999999) &
        (F.col("mmsi") < 1000000000) &
        (~F.col("mmsi").isin(111111111, 123456789, 999999999)) &
        (F.col("sog").between(1.0, 40.0))
    )


    # ── bounding box
    lat_delta = 0.93
    lon_delta = 1.60

    df = df.filter(
        F.col("latitude").between(CENTER_LAT - lat_delta, CENTER_LAT + lat_delta) &
        F.col("longitude").between(CENTER_LON - lon_delta, CENTER_LON + lon_delta)
    )

    # ── haversine filter (only remaining subset)
    R = 6371.0

    phi1 = F.radians(F.col("latitude"))
    phi2 = F.radians(F.lit(CENTER_LAT))
    dphi = F.radians(F.col("latitude") - F.lit(CENTER_LAT))
    dlambda = F.radians(F.col("longitude") - F.lit(CENTER_LON))

    hav = (
        F.sin(dphi / 2) ** 2 +
        F.cos(phi1) * F.cos(phi2) * F.sin(dlambda / 2) ** 2
    )

    dist_km = 2 * R * F.atan2(F.sqrt(hav), F.sqrt(1 - hav))

    return df.withColumn("dist_to_center_km", dist_km) \
              .filter(F.col("dist_to_center_km") <= RADIUS_KM) \
              .drop("dist_to_center_km")


# ─────────────────────────────────────────────────────────────────────────────
def clean_anomalies(df):
    """
    Removes GPS noise + deduplicates into stable time buckets.
    """

    df = df.repartition("mmsi")

    win = Window.partitionBy("mmsi").orderBy("timestamp")

    df = df.withColumn("prev_lat", F.lag("latitude").over(win)) \
           .withColumn("prev_lon", F.lag("longitude").over(win)) \
           .withColumn("prev_ts", F.lag("timestamp").over(win))

    df = df.withColumn(
        "time_diff_sec",
        F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")
    )

    R = 6371.0

    phi1 = F.radians(F.col("latitude"))
    phi2 = F.radians(F.col("prev_lat"))
    dphi = F.radians(F.col("latitude") - F.col("prev_lat"))
    dlambda = F.radians(F.col("longitude") - F.col("prev_lon"))

    hav = (
        F.sin(dphi / 2) ** 2 +
        F.cos(phi1) * F.cos(phi2) * F.sin(dlambda / 2) ** 2
    )

    dist_km = 2 * R * F.atan2(F.sqrt(hav), F.sqrt(1 - hav))

    df = df.withColumn("dist_to_prev_km", dist_km)

    df = df.filter(
        F.col("prev_lat").isNull() |
        (
            (F.col("time_diff_sec") > 0) &
            (F.col("dist_to_prev_km") / F.col("time_diff_sec") <= 0.020578)
        )
    )

    df = df.drop("prev_lat", "prev_lon", "prev_ts",
                 "time_diff_sec", "dist_to_prev_km")

    # ── single consistent bucket definition (IMPORTANT FIX)
    df = df.withColumn(
        "time_bucket",
        (F.unix_timestamp("timestamp") / 30).cast("int")
    )

    df = df.dropDuplicates(["mmsi", "time_bucket"])

    return df