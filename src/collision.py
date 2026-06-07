import pyspark.sql.functions as F
from pyspark.sql.window import Window
import math

# Spatial bucketing
GRID_DEG            = 0.02   
THRESHOLD_KM        = 0.20    # 200m first-pass candidate radius
COLLISION_KM        = 0.15    # 150m true collision radius
TIME_BUCKET_SEC     = 30

# Track quality gates 
MIN_SOG             = 0.5     # knots
MIN_TIME_SPAN_S     = 0       # turned off, since short encounters may be more valid than longer ones
MAX_TIME_SPAN_S   = 1800
MIN_POINTS          = 2       # need at least 2 ping pairs

# V-shape gates 
V_DEPTH_KM          = 0.001   # minimum distance change
MIN_CLOSING_SPD     = 0.0 

# CPA gate
CPA_T_MAX_S         = 600
CPA_PARALLEL_EPS    = 1e-10
CPA_VALID_FRAC      = 0.10    # 10% of pings need valid CPA

# Parallel-vessel gates 
COG_DIFF_MIN_DEG    = 10.0
COG_FRAC            = 0.20    # 20% of pings must show heading difference
DIST_STD_MIN_KM     = 0.001   # 1 m stddev 

R_EARTH             = 6371.0

# Haversine function to compute distance
def _haversine(lat1, lon1, lat2, lon2):
    phi1    = F.radians(lat1);  phi2    = F.radians(lat2)
    dphi    = F.radians(lat2 - lat1)
    dlambda = F.radians(lon2 - lon1)
    hav = (F.sin(dphi / 2) ** 2
           + F.cos(phi1) * F.cos(phi2) * F.sin(dlambda / 2) ** 2)
    return 2 * R_EARTH * F.atan2(F.sqrt(hav), F.sqrt(1 - hav))


def _cog_to_xy(sog_col, cog_col):
    knots_to_km_s = F.lit(1.852 / 3600.0)
    cog_rad = F.radians(cog_col)
    return (sog_col * knots_to_km_s * F.sin(cog_rad),
            sog_col * knots_to_km_s * F.cos(cog_rad))


def _circular_cog_diff(cog_a, cog_b):
    raw = F.abs(cog_a - cog_b) % F.lit(360.0)
    return F.when(raw > 180.0, F.lit(360.0) - raw).otherwise(raw)


def find_collision(df):
    """
    Detect the most likely collision event in the dataset, returns dict or None.
    """

    # Null / SOG guard,  no COG filter 
    df = df.filter(
        F.col("timestamp").isNotNull() &
        F.col("latitude") .isNotNull() &
        F.col("longitude").isNotNull() &
        F.col("mmsi")     .isNotNull() &
        F.col("sog")      .isNotNull() &
        (F.col("sog") >= MIN_SOG) &
        F.col("latitude") .between(-90,   90) &
        F.col("longitude").between(-180, 180)
    )

    # Extra filtering for COG
    df = df.withColumn(
        "cog",
        F.when(
            F.col("cog").isNull() |
            F.col("cog").isin(360.0, 511.0) |
            ~F.col("cog").between(0, 359.9),
            F.lit(None).cast("double")
        ).otherwise(F.col("cog").cast("double"))
    )
    df = df.withColumn("cog_known", F.col("cog").isNotNull())
    df = df.withColumn("cog", F.coalesce(F.col("cog"), F.lit(0.0)))

 
    df = df.filter(
        ~F.lower(F.col("name")).rlike(r"pilot|danpilot|svitzer|\btug\b|loods|lootsman"))

    # Grid + time + velocity 
    df = (df
          .withColumn("ts",          F.unix_timestamp("timestamp"))
          .withColumn("lat_cell",   (F.col("latitude")  / GRID_DEG).cast("int"))
          .withColumn("lon_cell",   (F.col("longitude") / GRID_DEG).cast("int"))
          .withColumn("time_bucket",(F.col("ts") / TIME_BUCKET_SEC).cast("int")))

    vx_expr, vy_expr = _cog_to_xy(F.col("sog"), F.col("cog"))
    df = df.withColumn("vx", vx_expr).withColumn("vy", vy_expr)

    df = df.repartition("lat_cell", "lon_cell").cache()
    df.count()

    # Prefix columns 
    KEEP = ["mmsi", "timestamp", "ts", "latitude", "longitude",
            "sog", "cog", "cog_known", "vx", "vy",
            "name", "lat_cell", "lon_cell", "time_bucket"]

    va_base = df.select([F.col(c).alias(f"va_{c}") for c in KEEP])
    va = va_base.withColumn(
        "va_join_tb",
        F.explode(F.array(
            F.col("va_time_bucket") - 1,
            F.col("va_time_bucket"),
            F.col("va_time_bucket") + 1,
        ))
    )
    vb = df.select([F.col(c).alias(f"vb_{c}") for c in KEEP])

    # Joining
    joined = va.join(
        vb,
        (F.col("va_mmsi")     <  F.col("vb_mmsi"))     &
        (F.col("va_lat_cell") == F.col("vb_lat_cell")) &
        (F.col("va_lon_cell") == F.col("vb_lon_cell")) &
        (F.col("va_join_tb")  == F.col("vb_time_bucket"))
    ).drop("va_join_tb")

    # Haversine + threshold 
    joined = joined.withColumn(
        "dist_km",
        _haversine(F.col("va_latitude"), F.col("va_longitude"),
                   F.col("vb_latitude"), F.col("vb_longitude"))
    )
    close = joined.filter(F.col("dist_km") <= THRESHOLD_KM)
    close = close.dropDuplicates(["va_mmsi", "vb_mmsi", "va_ts"])
    close = close.withColumn(
        "pair_id", F.concat_ws("_", F.col("va_mmsi"), F.col("vb_mmsi"))
    )

    # COG diff + CPA per ping pair
    close = close.withColumn(
        "cog_diff", _circular_cog_diff(F.col("va_cog"), F.col("vb_cog"))
    )
    close = close.withColumn(
        "cog_differs",
        F.col("va_cog_known") & F.col("vb_cog_known") &
        (F.col("cog_diff") > COG_DIFF_MIN_DEG)
    )

    cos_lat = F.cos(F.radians((F.col("va_latitude") + F.col("vb_latitude")) / 2))
    close = (close
        .withColumn("dp_x",
            (F.col("vb_longitude") - F.col("va_longitude")) *
            (math.pi / 180.0) * R_EARTH * cos_lat)
        .withColumn("dp_y",
            (F.col("vb_latitude") - F.col("va_latitude")) *
            (math.pi / 180.0) * R_EARTH)
        .withColumn("dv_x", F.col("vb_vx") - F.col("va_vx"))
        .withColumn("dv_y", F.col("vb_vy") - F.col("va_vy"))
    )
    close = close.withColumn("dv2", F.col("dv_x")**2 + F.col("dv_y")**2)
    close = close.withColumn(
        "dot_pv", F.col("dp_x")*F.col("dv_x") + F.col("dp_y")*F.col("dv_y")
    )
    close = close.withColumn(
        "t_cpa_raw",
        F.when(F.col("dv2") > CPA_PARALLEL_EPS,
               -F.col("dot_pv") / F.col("dv2")
        ).otherwise(F.lit(None).cast("double"))
    )
    close = close.withColumn(
        "cpa_dist_km",
        F.when(
            F.col("t_cpa_raw").isNotNull() &
            (F.col("t_cpa_raw") >= 0) &
            (F.col("t_cpa_raw") <= CPA_T_MAX_S),
            F.sqrt(
                (F.col("dp_x") + F.col("dv_x") * F.col("t_cpa_raw"))**2 +
                (F.col("dp_y") + F.col("dv_y") * F.col("t_cpa_raw"))**2
            )
        ).otherwise(F.col("dist_km"))
    )
    close = close.withColumn(
        "cpa_valid",
        F.col("t_cpa_raw").isNotNull() &
        (F.col("t_cpa_raw") >= 0) &
        (F.col("t_cpa_raw") <= CPA_T_MAX_S) &
        (F.col("cpa_dist_km") <= COLLISION_KM)
    )

    # V-shape signals 
    w_pair = Window.partitionBy("pair_id").orderBy("va_timestamp")
    close = (close
             .withColumn("prev_dist",  F.lag ("dist_km").over(w_pair))
             .withColumn("next_dist",  F.lead("dist_km").over(w_pair))
             .withColumn("prev_va_ts", F.lag ("va_ts").over(w_pair)))
    close = close.withColumn(
        "closing_speed",
        F.when(
            F.col("prev_dist").isNotNull() &
            F.col("prev_va_ts").isNotNull() &
            (F.col("va_ts") > F.col("prev_va_ts")),
            (F.col("prev_dist") - F.col("dist_km")) /
            (F.col("va_ts") - F.col("prev_va_ts")).cast("double")
        ).otherwise(F.lit(0.0))
    )
    close = (close
        .withColumn("is_local_min",
            F.col("prev_dist").isNotNull() & F.col("next_dist").isNotNull() &
            (F.col("dist_km") < F.col("prev_dist")) &
            (F.col("dist_km") < F.col("next_dist")))
        .withColumn("is_approach",
            F.col("prev_dist").isNotNull() &
            (F.col("dist_km") < F.col("prev_dist")))
        .withColumn("is_departure",
            F.col("next_dist").isNotNull() &
            (F.col("dist_km") < F.col("next_dist")))
    )

    # Per-pair aggregation 
    pattern = close.groupBy("pair_id", "va_mmsi", "vb_mmsi").agg(
        F.count("*")                                         .alias("n_points"),
        F.sum(F.col("is_local_min").cast("int"))             .alias("num_minima"),
        F.max(F.col("is_approach") .cast("int"))             .alias("has_approach"),
        F.max(F.col("is_departure").cast("int"))             .alias("has_departure"),
        (F.max("va_ts") - F.min("va_ts"))                    .alias("time_span_s"),
        F.min("dist_km")                                     .alias("min_dist"),
        F.max("dist_km")                                     .alias("max_dist"),
        F.stddev("dist_km")                                  .alias("dist_stddev"),
        F.max("closing_speed")                               .alias("max_closing_spd"),
        F.sum(F.col("cpa_valid")   .cast("int"))             .alias("n_cpa_valid"),
        F.sum(F.col("cog_differs") .cast("int"))             .alias("n_cog_differs"),
        F.min(F.when(F.col("cpa_valid"), F.col("cpa_dist_km"))).alias("best_cpa_km"),
        F.first("va_name")                                   .alias("name1"),
        F.first("vb_name")                                   .alias("name2"),
    )

    pattern = pattern.withColumn(
        "v_depth_km",
        F.col("max_dist") - F.col("min_dist"))

    pattern = pattern.withColumn(
        "cpa_valid_frac", F.log1p(F.col("n_cpa_valid")) / F.log1p(F.col("n_points")))

    pattern = pattern.withColumn(
        "cog_diff_frac",
        F.col("n_cog_differs").cast("double") / F.col("n_points"))

    pattern = pattern.withColumn(
        "burst_compactness",
        F.col("n_points") / (F.col("time_span_s") + 1.0))

    pattern = pattern.withColumn(
        "collision_spike",
        F.when(
            F.col("min_dist") < 0.05,
            1.0 / (F.col("dist_stddev") + 1e-6)).otherwise(0.0))

    pattern = pattern.withColumn(
        "collision_score", (
            (1.0 / (F.col("min_dist") + 1e-6)) * 5.0 +     # stronger proximity weight
            F.pow(F.col("cpa_valid_frac"), 3) * 10.0 +
            F.col("v_depth_km") * 2.0 +               
            F.col("cog_diff_frac") * 1.5 +                
            F.when(F.col("time_span_s") > 600,  -20.0).otherwise(0.0)))

    pattern = pattern.withColumn(
        "collision_score",
        F.col("collision_score")
        + F.log1p(F.col("burst_compactness")) * 100.0
        + F.log1p(F.col("collision_spike")) * 10.0
    )

    pattern = pattern.withColumn(
        "collision_score",
        F.col("collision_score") +
        F.when(
            (F.col("min_dist") <= 0.03) &
            (F.col("best_cpa_km") <= 0.02),
            20.0
        ).otherwise(0.0))

    collision_signature = (
        (F.col("min_dist") < 0.05) &
        (F.col("v_depth_km") > 0.05) &
        (F.col("cpa_valid_frac") > 0.5))

    pattern = pattern.withColumn(
        "collision_score",
        F.when(collision_signature, F.col("collision_score") * 2.0).otherwise(F.col("collision_score")))

    # Collision bonus
    pattern = pattern.withColumn(
        "collision_score",
        F.col("collision_score") +
        F.when(
            (F.col("time_span_s")    <= 120) &
            (F.col("cog_diff_frac")  >= 0.8) &
            (F.col("cpa_valid_frac") >= 0.8),
            30.0).otherwise(0.0))
    
    
    # Qualification: having approach or local minimum
    qualified = pattern.filter(
        ((F.col("has_approach")   == 1) |
         (F.col("num_minima")     >= 1) |
         (F.col("has_departure")  == 1)) &
        (F.col("n_points")        >= MIN_POINTS)   &
        (F.col("time_span_s")     <= MAX_TIME_SPAN_S) &   # cap long-duration proximity
        (F.col("min_dist")        <= COLLISION_KM) &
        (F.col("v_depth_km")      >= V_DEPTH_KM)   &
        (F.col("max_closing_spd") >= MIN_CLOSING_SPD) &
        (F.col("cog_diff_frac")   >= COG_FRAC)     &
        (F.col("dist_stddev")     >= DIST_STD_MIN_KM) &
        (F.col("cpa_valid_frac")  >= CPA_VALID_FRAC))

    # ── 10. Diagnostics ───────────────────────────────────────────────────────
    print("\n>>> [DIAG] ALL pairs before qualification (ordered by min_dist):")
    pattern.select(
        "va_mmsi", "vb_mmsi", "name1", "name2",
        "n_points", "num_minima", "has_approach", "has_departure",
        "time_span_s", "min_dist", "v_depth_km",
        F.round("dist_stddev",    6).alias("dist_stddev"),
        F.round("cog_diff_frac",  3).alias("cog_diff_frac"),
        F.round("cpa_valid_frac", 3).alias("cpa_valid_frac"),
        F.round("max_closing_spd",6).alias("max_closing_spd"),
        "best_cpa_km"
    ).orderBy(F.asc("min_dist")).show(50, truncate=False)

    print("\n>>> Qualified collision candidates:")
    qualified.select(
        "va_mmsi", "vb_mmsi", "name1", "name2",
        "min_dist", "best_cpa_km", "v_depth_km", "dist_stddev",
        "cog_diff_frac", "cpa_valid_frac", "max_closing_spd",
        "n_points", "time_span_s"
    ).orderBy(F.asc("min_dist")).show(50, truncate=False)

    best = (qualified
        .orderBy(F.desc("collision_score"))
        .limit(20)
        .collect())

    for r in best:
        print(
            r["va_mmsi"], r["vb_mmsi"],
            f"min={r['min_dist']:.4f}km",
            f"cpa={r['best_cpa_km']}",
            f"v_depth={r['v_depth_km']:.4f}km",
            f"cog_frac={r['cog_diff_frac']:.2f}",
            f"cpa_frac={r['cpa_valid_frac']:.2f}",
        )

    if not best:
        return None

    r     = best[0]
    mmsi1 = r["va_mmsi"]
    mmsi2 = r["vb_mmsi"]

    # Closest-approach row
    collision_row = (
        close
        .filter((F.col("va_mmsi") == mmsi1) & (F.col("vb_mmsi") == mmsi2))
        .orderBy("dist_km")
        .select(
            F.col("va_timestamp").alias("collision_time"),
            F.col("va_latitude") .alias("lat"),
            F.col("va_longitude").alias("lon"),
            F.col("dist_km"),
            F.col("va_name")     .alias("name1"),
            F.col("vb_name")     .alias("name2"),
        )
        .limit(1)
        .collect()[0]
    )

    return {
        "mmsi1":           mmsi1,
        "mmsi2":           mmsi2,
        "name1":           collision_row["name1"],
        "name2":           collision_row["name2"],
        "collision_time":  collision_row["collision_time"],
        "lat":             collision_row["lat"],
        "lon":             collision_row["lon"],
        "distance_km":     collision_row["dist_km"],
        "confidence_hits": r["n_points"],
        "num_minima":      r["num_minima"],
        "time_span_s":     r["time_span_s"],
    }


def get_trajectories(df, collision_info):
    from pyspark.sql.functions import col, unix_timestamp as _uts
    c_ts = int(collision_info["collision_time"].timestamp())
    return (
        df
        .withColumn("ts", _uts("timestamp"))
        .filter(col("mmsi").isin(collision_info["mmsi1"], collision_info["mmsi2"]))
        .filter((col("ts") >= c_ts - 600) & (col("ts") <= c_ts + 600))
        .drop("ts")
        .orderBy("mmsi", "timestamp")
        .toPandas()
    )
