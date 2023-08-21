from typing import List
import polyline
from shapely.geometry import LineString
import geopandas as gp
import pandas as pd
import numpy as np
from shapely.geometry import Point
from pathlib import Path
import requests
import strava

DIR = Path(__file__).parent.absolute()


def read_gdf(name):
    return gp.read_file(DIR / "data" / name).to_crs(epsg=26986)


def write_gdf(gdf, filename):
    filepath = DIR / "data" / filename
    with open(filepath, "w") as f:
        f.write(gdf.to_crs(epsg=4326).to_json())
    print(f"Saved: {filepath}")


def snap(points, lines, tolerance=10, sindex_offset=15, only_one=False):
    """
    Args:
        points: A GeoDataFrame of point geometries.
        lines: A GeoDataFrame of line geometries.
        tolerance: The max distance (in CRS units) for a point to be snapped.
        sindex_offset: Distance for spatial index bounding box.
        only_one: `True` if a point should only snap to one line, otherwise a point
            will snap to the closest location on each line within the tolerance.

    Returns:
        A GeoDataFrame containing all columns from the `points` and `lines` GeoDataFrames,
        where each point is snapped to the closest location on each line within `tolerance`
        distance. The GeoDataFrame geometry is the snapped location, while the original
        geometries are in the `point_geometry` and `line_geometry` columns. Points can be
        snapped to multiple lines within `tolerance` distance, unless `only_one = True`. The
        distance between the original and snapped point is in the `snap_distance` column.

        ADDS point_projection!!

    Raises:
        None
    """
    # Find nearby lines using spatial index
    bbox = points.bounds + [-sindex_offset, -sindex_offset, sindex_offset, sindex_offset]
    hits = bbox.apply(lambda row: list(lines.sindex.intersection(row)), axis=1)

    # Created GeoDataFrame joining points and nearby lines (use line as geometry)
    tmp = pd.DataFrame(
        {
            "point_index": np.repeat(hits.index, hits.apply(len)),
            "line_index": np.concatenate(hits.values),
        }
    )
    tmp = tmp.join(lines.reset_index(drop=True), on="line_index")
    tmp = tmp.join(points.rename(columns={"geometry": "point_geometry"}), on="point_index")
    tmp = gp.GeoDataFrame(tmp, crs=points.crs)

    # Calculate distance from point to line. Only keep those within tolerance
    tmp["snap_distance"] = tmp.geometry.distance(
        gp.GeoSeries(tmp.point_geometry, crs=tmp.crs)
    )
    tmp = tmp.loc[tmp.snap_distance <= tolerance]

    # If snapping to a single line, only keep the closest snap
    if only_one:
        tmp = tmp.sort_values(by=["snap_distance"]).groupby("point_index").first()

    # Set geometry as the snapped location. Keep the line as `line_geometry`
    tmp["line_geometry"] = tmp.geometry
    tmp["point_projection"] = tmp.line_geometry.project(gp.GeoSeries(tmp.point_geometry))
    tmp.geometry = tmp.line_geometry.interpolate(tmp.point_projection)

    return tmp.drop(["line_index", "point_index"], axis=1)


def get_complete(gdf: gp.GeoDataFrame):
    """ """
    def largest_gap(series):
        return series.sort_values().diff().max()
    gdf = (
        gdf.groupby(["street_id", "strava_id", "line_length"])
        .point_projection.agg(proj_min="min", proj_max="max", max_gap=largest_gap)
        .reset_index()
    )
    gdf = gdf[(gdf.max_gap < 40) & ((gdf.proj_max - gdf.proj_min) / gdf.line_length > 0.55)]
    return set(gdf.street_id)


def interpolate_points(points: List[Point], max_segment_length):
    """
    Adds points such that no segment is longer than `max_segment_length`
    """
    new_points = [points[0]]
    for i in range(1, len(points)):
        p1, p2 = points[i - 1], points[i]
        segment_length = p1.distance(p2)
        if segment_length > max_segment_length:
            num_intermediate_points = int(segment_length / max_segment_length)
            x_step = (p2.x - p1.x) / (num_intermediate_points + 1)
            y_step = (p2.y - p1.y) / (num_intermediate_points + 1)
            for j in range(1, num_intermediate_points + 1):
                new_x = p1.x + j * x_step
                new_y = p1.y + j * y_step
                new_points.append(Point(new_x, new_y))
        new_points.append(p2)
    return new_points


def extract_points(activities=None, activity=None, linestring=None, max_segment_length=15):
    """
    Extracts the points in a linestring, Strava activity, or GeoDataFrame of Strava
    activities. Strava activities will retain the activity ID.
    """
    provided_count = sum(p is not None for p in [linestring, activity, activities])
    if provided_count != 1:
        raise ValueError("Exactly one parameter should be provided.")

    if linestring is not None:
        points = interpolate_points(
            [Point(pt) for pt in linestring.coords], max_segment_length
        )
        return gp.GeoDataFrame(geometry=points)
    elif activity is not None:
        return (
            extract_points(
                linestring=activity.geometry, max_segment_length=max_segment_length
            )
            .reset_index()
            .assign(strava_id=activity.id)
        )
    elif activities is not None:
        return pd.concat(
            activities.apply(
                lambda x: extract_points(activity=x, max_segment_length=max_segment_length),
                axis=1,
            ).to_list(),
            ignore_index=True,
        )


def update_geojson():
    activities, pageNum = [], 1
    while True:
        res = requests.get(
            f"https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {strava.access_token()}"},
            params={"per_page": 100, "page": pageNum},
        )
        activities.extend(res.json())
        if len(res.json()) < 100:
            break
        pageNum += 1

    lines = [polyline.decode(a["map"]["summary_polyline"]) for a in activities]
    lines = [LineString([(lon, lat) for lat, lon in line]) for line in lines]
    gdf = gp.GeoDataFrame(activities, geometry=lines, crs="EPSG:4326")

    write_gdf(gdf, "activities.geojson")

    return gdf.to_crs(epsg=26986)


def run(towns, activity_types):
    outlines = read_gdf("outlines.geojson")
    outlines = outlines[outlines["TOWN"].isin(towns)][["TOWN", "geometry"]]

    activities = update_geojson()[["id", "geometry", "type"]]
    activities = activities[activities["type"].isin(activity_types)]
    activities = (
        gp.sjoin(activities, outlines, predicate="intersects")
        .drop_duplicates(subset="id")[["id", "geometry"]]
        .reset_index(drop=True)
    )
    write_gdf(activities, "filtered_activities.geojson")

    points = extract_points(activities, max_segment_length=15)
    centerlines = read_gdf("centerlines.geojson").rename(columns={"index": "street_id"})
    centerlines = centerlines[centerlines["TOWN"].isin(towns)]    
    centerlines["line_length"] = centerlines.geometry.length
    write_gdf(centerlines, "filtered_centerlines.geojson")

    snapped = snap(points, centerlines)
    complete = centerlines[centerlines.street_id.isin(get_complete(snapped))]
    write_gdf(complete, "completed_centerline.geojson")

    summary = pd.concat(
        [
            complete.groupby("TOWN")["geometry"].agg(lambda geoms: geoms.length.sum()),
            centerlines.groupby("TOWN")["geometry"].agg(lambda geoms: geoms.length.sum()),
        ],
        axis=1,
        keys=["completed", "total"],
    ).fillna(0)
    summary["percent"] = summary.apply(lambda row: row["completed"] / row["total"], axis=1)
    print(summary)


if __name__ == "__main__":
    run(
        towns = ["SOMERVILLE", "CAMBRIDGE", "MEDFORD", "BOSTON"],
        activity_types = ["Run", "Ride"],
    )