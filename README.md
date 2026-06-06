# Big-Data-Exam

Detects the closest-approach collision event between two vessels in the
Danish AIS dataset for **December 2021** within a **50 nm radius** of
`55.225 °N, 14.245 °E`, using **PySpark** inside a **Docker** container.

---

## Repository Layout

```
.
├── src/
│   ├── main.py           # Pipeline entry point
│   ├── preprocessing.py  # Data loading, filtering, anomaly removal
│   ├── collision.py      # Grid-bucketed collision detection
│   └── visualization.py  # Trajectory map generation
├── data/
│   └── raw/extracted/    # ← place CSV files here (see Data Setup)
├── output/               # Generated results (git-ignored)
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
├── requirements.txt
└── README.md
```

---

### 1. Data Setup

Downloaded the Danish AIS daily CSV files from
<http://aisdata.ais.dk/> for December 2021 and placed them under
`./data/raw/extracted/`:

```
data/raw/extracted/aisdk-2021-12-01.csv
data/raw/extracted/aisdk-2021-12-02.csv
...
data/raw/extracted/aisdk-2021-12-31.csv
```

### 2. Build the Docker Image

```bash
docker build -t ais-collision:latest .
```

### 3. Run

**Single-day test:**

```bash
docker compose run --rm ais-collision \
  spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    --conf spark.sql.shuffle.partitions=50 \
    /app/src/main.py \
    "/app/data/raw/extracted/aisdk-2021-12-01.csv"
```

**Multi-day run (edit the glob):**

```bash
docker compose run --rm ais-collision \
  spark-submit \
    --master "local[*]" \
    --driver-memory 8g \
    --conf spark.sql.shuffle.partitions=200 \
    /app/src/main.py \
    "/app/data/raw/extracted/*.csv"
```

**Via docker-compose defaults** (edit `command:` in `docker-compose.yml`
then run):

```bash
docker compose up
```

### 5. Output

After a successful run, two files appear in `./output/`:

| File | Contents |
|------|----------|
| `results.txt` | MMSI numbers, vessel names, timestamp, coordinates, distance |
| `collision_trajectory.png` | Trajectory map ±10 min around the collision |

---

## Methodology

### Data Loading

Raw CSV files are ingested with `spark.read.csv(..., header=True,
inferSchema=True)`.  Column names are normalised to `snake_case` and a
synthetic `name` column is added when absent.

### Filtering

1. **Navigational gate** – retains Class A/B vessels under way with
   `1.0 ≤ SOG ≤ 40.0` knots.
3. **Bounding box** – rejects rows outside a rectangular envelope around the
   50 nm circle (cheap column comparison, no trigonometry).
4. **Haversine circle** – exact great-circle distance to the centre point;
   discards everything beyond 92.6 km (50 nm).

### Anomaly / Noise Removal

GPS ghost points are detected by comparing each ping to the previous ping
for the same vessel.  If the implied speed exceeds **60 knots**
(0.020578 km/s) the point is a teleportation artefact and is dropped.

Duplicate pings in the same 30-second bucket per MMSI are removed by
`dropDuplicates(["mmsi", "time_bucket"])`.

### Collision Detection

1. **Grid cells** – each coordinate is mapped to a 0.02° × 0.02° cell
   (~1.5 km at 55 °N).
2. **Time buckets** – timestamps are snapped to 30-second buckets.
3. **Neighbourhood join** – the self-join is restricted to rows in the
   same or **adjacent** cells (±1 in lat, lon, and time), giving a 3×3×3
   neighbourhood.  This reduces the join output from O(n²) to O(n × k)
   where k is the small number of vessels in nearby cells.
4. **Haversine threshold** – only pairs within 100 m pass.
5. **Collision signature** – a genuine collision shows a *local distance
   minimum* (vessels approach then depart).  Pairs that accumulate at
   least one such minimum with ≥ 3 data points are retained.
6. **Ranking** – pairs are sorted by (most minima, smallest min distance,
   most data points); the top pair is returned.

### Trajectory Extraction

The ±10-minute window is extracted by filtering on
`unix_timestamp ∈ [collision_ts − 600, collision_ts + 600]` for both MMSIs.

### Visualisation

A Matplotlib figure marks the approach (solid line) and departure (dashed
line) of each vessel with different colours.  The exact collision point is
marked with a gold star and annotated with timestamp, coordinates, and
inter-vessel distance.

---

## Results

After running the full pipeline the terminal prints and `output/results.txt`
contains:

```
COLLISION DETECTION RESULTS
========================================

Vessel A  : <name>
MMSI A    : <mmsi>
Vessel B  : <name>
MMSI B    : <mmsi>
Timestamp : <ISO timestamp>
Latitude  : <degrees N>
Longitude : <degrees E>
Distance  : <metres>
```

---
