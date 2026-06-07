import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd

# Set to False if the cluster has no outbound internet access
USE_BASEMAP = True

if USE_BASEMAP:
    try:
        import contextily as cx
        from pyproj import Transformer
        _BASEMAP_AVAILABLE = True
    except ImportError:
        print("    [viz] contextily / pyproj not installed — falling back to plain grid.")
        print("          Install with:  pip install contextily pyproj")
        _BASEMAP_AVAILABLE = False
else:
    _BASEMAP_AVAILABLE = False


def _to_webmercator(lons, lats):
    """Convert arrays of lon/lat (WGS-84) to Web Mercator (EPSG:3857)."""
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    xs, ys = transformer.transform(lons, lats)
    return xs, ys


def plot_trajectories(traj_pandas: pd.DataFrame,
                      collision_info: dict,
                      output_path: str) -> None:
    """
    Generate and save the trajectory visualisation.
    """
    mmsi1  = collision_info["mmsi1"]
    name1  = collision_info["name1"]
    mmsi2  = collision_info["mmsi2"]
    name2  = collision_info["name2"]
    c_time = collision_info["collision_time"]
    c_lat  = collision_info["lat"]
    c_lon  = collision_info["lon"]

    use_map = _BASEMAP_AVAILABLE

    fig, ax = plt.subplots(figsize=(13, 9))

    vessel_styles = [
        (mmsi1, name1, "#1f77b4", "#aec7e8"),   # blue shades
        (mmsi2, name2, "#d62728", "#f5b7b1"),   # red  shades
    ]

    # Collect all coordinates for extent calculation
    all_lons, all_lats = [c_lon], [c_lat]

    for mmsi, label, color_pre, color_post in vessel_styles:
        vdf = traj_pandas[traj_pandas["mmsi"] == mmsi].sort_values("timestamp")
        if vdf.empty:
            continue

        all_lons.extend(vdf["longitude"].tolist())
        all_lats.extend(vdf["latitude"].tolist())

        pre  = vdf[vdf["timestamp"] <= c_time]
        post = vdf[vdf["timestamp"] >  c_time]

        if use_map:
            # Plot in Web Mercator
            def _plot_line(sub, **kwargs):
                if sub.empty:
                    return
                xs, ys = _to_webmercator(sub["longitude"].values,
                                         sub["latitude"].values)
                ax.plot(xs, ys, **kwargs)

            def _plot_scatter(sub, idx, **kwargs):
                if sub.empty:
                    return
                xs, ys = _to_webmercator([sub["longitude"].iloc[idx]],
                                         [sub["latitude"].iloc[idx]])
                ax.scatter(xs, ys, **kwargs)

            _plot_line(vdf,  color=color_post, linewidth=1.2, linestyle="-", zorder=2)
            if not pre.empty:
                _plot_line(pre, color=color_pre, linewidth=2.5, linestyle="-",
                           label=f"{label} ({mmsi}) – approach", zorder=3)
                _plot_scatter(pre,  0,  color=color_pre, s=90, marker="^", zorder=5)
            if not post.empty:
                _plot_line(post, color=color_pre, linewidth=2.5, linestyle="--",
                           label=f"{label} ({mmsi}) – departure", zorder=3)
                _plot_scatter(post, -1, color=color_pre, s=90, marker="v", zorder=5)
        else:
            # Plain lat/lon plot
            ax.plot(vdf["longitude"], vdf["latitude"],
                    color=color_post, linewidth=1.2, linestyle="-", zorder=2)
            if not pre.empty:
                ax.plot(pre["longitude"], pre["latitude"],
                        color=color_pre, linewidth=2.5, linestyle="-",
                        label=f"{label} ({mmsi}) – approach", zorder=3)
                ax.scatter(pre["longitude"].iloc[0], pre["latitude"].iloc[0],
                           color=color_pre, s=90, marker="^", zorder=5)
            if not post.empty:
                ax.plot(post["longitude"], post["latitude"],
                        color=color_pre, linewidth=2.5, linestyle="--",
                        label=f"{label} ({mmsi}) – departure", zorder=3)
                ax.scatter(post["longitude"].iloc[-1], post["latitude"].iloc[-1],
                           color=color_pre, s=90, marker="v", zorder=5)

    # Collision point 
    if use_map:
        cx_pt, cy_pt = _to_webmercator([c_lon], [c_lat])
        ax.scatter(cx_pt, cy_pt,
                   color="gold", edgecolors="black", linewidths=1.2,
                   marker="*", s=400, zorder=6, label="Collision point")
        ann_xy    = (cx_pt[0], cy_pt[0])
        ann_xytext= (cx_pt[0] + 80, cy_pt[0] + 80)   #meters offset
    else:
        ax.scatter(c_lon, c_lat,
                   color="gold", edgecolors="black", linewidths=1.2,
                   marker="*", s=400, zorder=6, label="Collision point")
        ann_xy    = (c_lon, c_lat)
        ann_xytext= (c_lon + 0.003, c_lat + 0.003)

    # Format collision timestamp
    c_time_str = (c_time.strftime("%Y-%m-%d %H:%M:%S UTC")
                  if hasattr(c_time, "strftime") else str(c_time))

    ax.annotate(
        f"  {c_time_str}\n"
        f"  {c_lat:.5f}°N  {c_lon:.5f}°E\n"
        f"  Δ {round(collision_info['distance_km'] * 1000, 1)} m",
        xy=ann_xy, xytext=ann_xytext,
        fontsize=8,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow",
                  edgecolor="grey", alpha=0.90),
        arrowprops=dict(arrowstyle="->", color="grey"),
        zorder=7
    )

    # OSM basemap
    if use_map:
        try:
            cx.add_basemap(ax, crs="EPSG:3857",
                           source=cx.providers.OpenStreetMap.Mapnik,
                           zoom=14, alpha=0.7)
            ax.set_axis_off()   
        except Exception as e:
            print(f"    [viz] Basemap fetch failed ({e}); continuing without tiles.")
            ax.grid(True, linestyle="--", alpha=0.5)
            ax.set_xlabel("Web Mercator X (m)")
            ax.set_ylabel("Web Mercator Y (m)")
    else:
        ax.grid(True, linestyle="--", alpha=0.5, linewidth=0.7)
        ax.set_xlabel("Longitude (°E)", fontsize=11)
        ax.set_ylabel("Latitude (°N)",  fontsize=11)

    # Title + legend
    ax.set_title(
        f"Vessel Collision  –  ±10 min window\n"
        f"{name1} ({mmsi1})  vs  {name2} ({mmsi2})\n"
        f"{c_time_str}",
        fontsize=12, fontweight="bold", pad=10
    )

    solid_patch  = mpatches.Patch(color="grey", label="— approach")
    dashed_patch = mpatches.Patch(color="grey", linestyle="--", label="╌ departure")
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles + [solid_patch, dashed_patch],
              labels  + ["— approach", "╌ departure"],
              loc="best", fontsize=8, framealpha=0.9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    Visualisation saved → {output_path}")
