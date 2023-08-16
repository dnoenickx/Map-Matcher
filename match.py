import polyline
from shapely.geometry import LineString
import geopandas as gp
import pandas as pd
import numpy as np
from shapely.geometry import Point
from pathlib import Path
import requests

DIR = Path(__file__).parent.absolute()


def read_gdf(name):
    return gp.read_file(DIR / "data" / name).to_crs(epsg=26986)


def write_gdf(gdf, filename):
    filepath = DIR / "data" / filename
    with open(filepath, "w") as f:
        f.write(gdf.to_crs(epsg=4326).to_json())
    print(f"Saved: {filepath}")


def gpx_to_gdf(filepath=None, gpx=None):
    assert bool(gpx) ^ bool(filepath)
    if filepath:
        with open(filepath) as f:
            gpx = gpxpy.parse(f)
        # gpx.reduce_points(min_distance=2)
    rows = []
    for track_no, track in enumerate(gpx.tracks if gpx.tracks else []):
        for segment_no, segment in enumerate(track.segments if track.segments else []):
            for point_no, point in enumerate(segment.points if segment.points else []):
                rows.append((point.longitude, point.latitude))
    df = pd.DataFrame(rows, columns=("longitude", "latitude"))
    return gp.GeoDataFrame(
        geometry=gp.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )


def snap(points, lines):
    """
    Returns a GeoDataFrame combining the columns of points and lines, where each row
    contains the information for a point and the line it was snapped to. The geometry is
    the snapped location. (Includes columns `snap_dist`, `orig_pt`, and `orig_line`)
    """
    offset = 15
    tolerance = 10
    #
    # Use with a spatial index and find nearby lines
    bbox = points.bounds + [-offset, -offset, offset, offset]
    hits = bbox.apply(lambda row: list(lines.sindex.intersection(row)), axis=1)
    #
    # Join points with nearby lines
    tmp = pd.DataFrame(
        {
            # index of points table
            "pt_idx": np.repeat(hits.index, hits.apply(len)),
            # ordinal position of line - access via iloc later
            "line_i": np.concatenate(hits.values),
        }
    )
    tmp = tmp.join(lines.reset_index(drop=True), on="line_i").drop("line_i", axis=1)
    tmp = tmp.join(points.rename(columns={"geometry": "orig_pt"}), on="pt_idx")
    #
    # Find the closest line to each point
    tmp = gp.GeoDataFrame(tmp, geometry="geometry", crs=points.crs)
    tmp["snap_dist"] = tmp.geometry.distance(gp.GeoSeries(tmp.orig_pt))
    tmp = tmp.loc[tmp.snap_dist <= tolerance]
    # tmp = tmp.sort_values(by=["snap_dist"]).groupby("pt_idx").first()
    tmp["line_length"] = tmp.geometry.length
    orig_lines = tmp.geometry
    tmp["proj"] = orig_lines.project(gp.GeoSeries(tmp.orig_pt))
    tmp.geometry = orig_lines.interpolate(tmp.proj)
    tmp["orig_line"] = orig_lines
    return tmp


def get_complete(gdf: gp.GeoDataFrame):
    """
    points: gdf of points
    lines: gdf of lines
    line_id: the column name that connects points and lines
    """
    coverage_threshold = 0.55
    gap_limit = 40
    complete = list()
    for (street_id, street_len, _), group in gdf.groupby(
        ["street_id", "line_length", "strava_id"]
    ):
        proj = group["proj"].sort_values()
        coverage = (proj.max() - proj.min()) / street_len
        max_gap = (
            proj.tail(-1).reset_index(drop=True) - proj.head(-1).reset_index(drop=True)
        ).max()
        # print(street_id, round(coverage, 4), round(max_gap, 4))
        if coverage < coverage_threshold or max_gap > gap_limit:
            continue
        complete.append(street_id)
    print(len(complete))
    return complete


def add_points(points, max_segment_length=15):
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


def extract_points(activities=None, activity=None, linestring=None):
    provided_count = sum(p is not None for p in [linestring, activity, activities])
    if provided_count != 1:
        raise ValueError("Exactly one parameter should be provided.")

    if linestring is not None:
        points = add_points([Point(pt) for pt in linestring.coords])
        return gp.GeoDataFrame(geometry=points)
    elif activity is not None:
        return (
            extract_points(linestring=activity.geometry)
            .reset_index()
            .assign(strava_id=activity.id)
        )
    elif activities is not None:
        return pd.concat(
            activities.apply(lambda x: extract_points(activity=x), axis=1).to_list(),
            ignore_index=True,
        )


def update_geojson(access_token):
    activities, pageNum = [], 1
    while True:
        res = requests.get(
            f"https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": 100, "page": pageNum},
        )
        activities.extend(res.json())
        if len(res.json()) < 100:
            break
        pageNum += 1

    lines = [polyline.decode(a["map"]["summary_polyline"]) for a in activities]
    lines = [LineString([(lon, lat) for lat, lon in line]) for line in lines]
    gdf = gp.GeoDataFrame(activities, geometry=lines, crs='EPSG:4326')

    write_gdf(gdf, "activities.geojson")

    return gdf


def find_majority_polygon(line_geometry, polygons, polygons_sindex):
    possible_matches_index = list(
        polygons_sindex.intersection(line_geometry.geometry.bounds)
    )
    possible_matches = polygons.iloc[possible_matches_index]
    intersecting_polygons = possible_matches[
        possible_matches.intersects(line_geometry.geometry)
    ]
    if not intersecting_polygons.empty:
        lengths = [
            line_geometry.geometry.intersection(polygon.geometry).length
            for _, polygon in intersecting_polygons.iterrows()
        ]
        max_length_idx = lengths.index(max(lengths))
        return intersecting_polygons.iloc[max_length_idx].name


def run(access_token):
    towns = ["SOMERVILLE", "CAMBRIDGE", "MEDFORD", "BOSTON"]

    outlines = read_gdf("outlines.geojson")
    outlines = outlines[outlines["TOWN"].isin(towns)]

    # # activities = update_geojson(access_token)[["id", "geometry"]]
    # activities = read_gdf("activities.geojson")[["id", "geometry"]]
    # # activities = activities[activities["id"].isin([7909733698, 9603008455])]
    # activities = (
    #     gp.sjoin(activities, outlines, op="intersects")
    #     .drop_duplicates(subset="id")[["id", "geometry"]]
    #     .reset_index(drop=True)
    # )

    # points = extract_points(activities)

    # centerlines = read_gdf("centerlines.geojson").drop('index', axis=1)  # .dissolve(by='geometry')
    centerlines = read_gdf("centerlines.geojson").rename(columns={"index": "street_id"})
    centerlines = centerlines[centerlines["TOWN"].isin(towns)]

    # snapped = snap(points, centerlines)
    # complete = centerlines[centerlines.street_id.isin(get_complete(snapped))]
    # # complete = get_complete(snapped)

    # summary = pd.concat(
    #     [
    #         complete.groupby("TOWN")["geometry"].agg(
    #             lambda geoms: geoms.length.sum()
    #         ),
    #         centerlines.groupby("TOWN")["geometry"].agg(
    #             lambda geoms: geoms.length.sum()
    #         ),
    #     ],
    #     axis=1,
    #     keys=["completed", "total"]
    # ).fillna(0)
    # summary['percent'] = summary.apply(lambda row: row['completed'] / row['total'], axis=1)
    # print(summary)


    write_gdf(centerlines, "filtered_centerlines.geojson")
    # write_gdf(activities, "filtered_activities.geojson")
    # write_gdf(complete, "completed_centerline.geojson")

if __name__ == "__main__":
    run(None)

# project() gives us the length from the start of the line to the closest point on that
# line to our original location. You can also use this for handy things like figuring
# out where to cut a line by a point.

# interpolate() takes that distance and gives us back a new point at the right location

# The project() method is the inverse of interpolate().
