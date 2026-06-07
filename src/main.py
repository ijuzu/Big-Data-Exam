"""
Usage (inside container):
  spark-submit --master local[*] --driver-memory 4g src/main.py
  spark-submit --master local[*] --driver-memory 4g src/main.py /app/data/raw/extracted/aisdk-2021-12-01.csv
  spark-submit --master local[*] --driver-memory 4g src/main.py "/app/data/raw/extracted/aisdk-2021-12-0[12].csv"

If no path argument is given, all CSV files in the default directory are used.
"""

import os
import sys

# Make src/ importable regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from preprocessing import load_data, filter_area_and_time, clean_anomalies
from collision import find_collision, get_trajectories
from visualization import plot_trajectories

def build_spark():
    return (SparkSession.builder
            .appName("AIS_Collision_Detection")
            .config("spark.driver.memory",            "4g")
            .config("spark.driver.maxResultSize",     "2g")
            .config("spark.sql.shuffle.partitions",   "50")
            .config("spark.sql.adaptive.enabled",          "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate())

def main():
    output_dir = os.getenv("OUTPUT_DIR", "output")
    os.makedirs(output_dir, exist_ok=True)

    data_path = sys.argv[1:] if len(sys.argv) > 1 else ["/app/data/raw/extracted/*.csv"]

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    SEP = "=" * 60
    print(f"\n{SEP}")
    print("  AIS VESSEL COLLISION DETECTION")
    print(f"  Data : {data_path}")
    print(f"{SEP}\n")

    print("Loading data...")
    raw_df = load_data(spark, data_path)
    print(raw_df.columns)

   #Filtering
    print("Filtering...")
    filtered_df = filter_area_and_time(raw_df)
    filtered_count = filtered_df.count()
    print(f"    Records retained : {filtered_count:,}")

    if filtered_count == 0:
        print("\nERROR: No records survived filtering.")
        spark.stop()
        sys.exit(1)

    # Anomaly removal 
    print("Removing stationary vessels and GPS anomalies …")
    clean_df = clean_anomalies(filtered_df)
    clean_df.cache()
    clean_count = clean_df.count()
    print(f"    Clean records    : {clean_count:,}")

    if clean_count == 0:
        print("\nERROR: All records removed during cleaning.")
        print("  • Check the SOG column name / values in your CSV files.")
        spark.stop()
        sys.exit(1)

    # ── Stage 4: Collision detection ─────────────────────────────────────────
    print(">>> [4/5] Running grid-bucketed collision search …")
    collision_info = find_collision(clean_df)

    if not collision_info:
        print("\nNo collision found within current threshold (0.5 km).")
        print("Suggestions:")
        print("  • Increase THRESHOLD_KM in collision.py and re-run.")
        print("  • Add more days to the data path.")
        spark.stop()
        sys.exit(0)

    dist_m = round(collision_info["distance_km"] * 1000, 1)

    # Format timestamp cleanly for display
    collision_ts = collision_info["collision_time"]
    collision_ts_str = (
        collision_ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        if hasattr(collision_ts, "strftime")
        else str(collision_ts)
    )

    print(f"\n{SEP}")
    print("  *** COLLISION EVENT DETECTED ***")
    print(f"{SEP}")
    print(f"  Vessel A  : {collision_info['name1']}")
    print(f"  MMSI A    : {collision_info['mmsi1']}")
    print(f"  Vessel B  : {collision_info['name2']}")
    print(f"  MMSI B    : {collision_info['mmsi2']}")
    print(f"  Timestamp : {collision_ts_str}")          # ← collision time
    print(f"  Latitude  : {collision_info['lat']:.6f}")
    print(f"  Longitude : {collision_info['lon']:.6f}")
    print(f"  Distance  : {dist_m} m")
    print(f"  CPA hits  : {collision_info['confidence_hits']} ping-pairs")
    print(f"  Time span : {collision_info['time_span_s']} s")
    print(f"{SEP}\n")

    # ── Stage 5: Trajectories + visualisation ────────────────────────────────
    print(">>> [5/5] Extracting ±10-min trajectories and rendering map …")
    traj_pandas = get_trajectories(clean_df, collision_info)
    print(f"    Trajectory points : {len(traj_pandas)}")

    plot_path = os.path.join(output_dir, "collision_trajectory.png")
    plot_trajectories(traj_pandas, collision_info, plot_path)

    # ── Persist text results ──────────────────────────────────────────────────
    results_path = os.path.join(output_dir, "results.txt")
    with open(results_path, "w") as f:
        f.write("COLLISION DETECTION RESULTS\n")
        f.write("=" * 40 + "\n\n")
        f.write(f"Vessel A  : {collision_info['name1']}\n")
        f.write(f"MMSI A    : {collision_info['mmsi1']}\n")
        f.write(f"Vessel B  : {collision_info['name2']}\n")
        f.write(f"MMSI B    : {collision_info['mmsi2']}\n")
        f.write(f"Timestamp : {collision_ts_str}\n")      # ← collision time
        f.write(f"Latitude  : {collision_info['lat']:.6f}\n")
        f.write(f"Longitude : {collision_info['lon']:.6f}\n")
        f.write(f"Distance  : {dist_m} m\n")
        f.write(f"CPA hits  : {collision_info['confidence_hits']} ping-pairs\n")
        f.write(f"Time span : {collision_info['time_span_s']} s\n")

    print(f"\n    Map    → {plot_path}")
    print(f"    Report → {results_path}")
    print("\n>>> Pipeline complete.\n")

    spark.stop()


if __name__ == "__main__":
    main()