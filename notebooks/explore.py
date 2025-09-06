import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import h3
    import os
    import polars as pl
    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
    import plotly.express as px
    import folium
    import marimo as mo
    return folium, mo, os


@app.cell
def _(os):
    # create data directories
    os.makedirs("./data/objects", exist_ok=True)
    os.makedirs("./data/people", exist_ok=True)
    return


@app.cell
def _(os):
    # delete all files in data directories
    for file in os.listdir("./data/objects"):
        os.remove(os.path.join("./data/objects", file))
    for file in os.listdir("./data/people"):
        os.remove(os.path.join("./data/people", file))
    return


@app.cell
def _(mo):
    orders_counts = mo.sql(
        f"""
        SELECT status, count(*) as count
        FROM './data/orders/*.parquet'
        GROUP BY status
        """
    )
    return


@app.cell
def _(mo):
    order_lines_counts = mo.sql(
        f"""
        SELECT status, count(*) as count
        FROM './data//order_lines/*.parquet'
        GROUP BY status
        """
    )
    return


@app.cell
def _(mo):
    orders_counts = mo.sql(
        f"""
        SELECT status, count(*) as count
        FROM './data//orders/*.parquet'
        GROUP BY status
        """
    )
    return


@app.cell
def _(mo):
    object_counts = mo.sql(
        f"""
        SELECT label, count(*) as count
        FROM './data//objects/*.parquet'
        GROUP BY label
        """
    )
    return


@app.cell
def _(mo):
    people_counts = mo.sql(
        f"""
        SELECT role, count(*) as count
        FROM './data/population/people/*.parquet'
        GROUP BY role
        """
    )
    return


@app.cell
def _(mo):
    trips = mo.sql(
        f"""
        SELECT
            -- workaround to not panic during output
            people.id::VARCHAR as id,
            MIN(pos.timestamp) as start_time,
            MAX(pos.timestamp) as end_time,
            ARRAY_AGG(
                pos.timestamp
                ORDER BY
                    pos.timestamp
            ) as timestamps,
            ARRAY_AGG(
                pos.position
                ORDER BY
                    pos.timestamp
            ) as positions
        FROM
            './data/population/people/*.parquet' as people
            LEFT JOIN (
                SELECT
                    *,
                    position.x - lag (position.x) OVER (
                        PARTITION BY
                            id
                        ORDER BY
                            timestamp
                    ) as diff
                FROM
                    './data/population/positions/*.parquet'
            ) as pos ON people.id = pos.id
        WHERE
            role == 'courier'
            AND abs(diff) > 1e-10
        GROUP BY
            people.id
        LIMIT
            10
        """
    )
    return (trips,)


@app.cell
def _():
    import datetime

    from typing import TypedDict, Iterable, Any


    class TripPoint(TypedDict):
        x: float
        y: float


    class TripData(TypedDict):
        timestamps: list[datetime.datetime]
        positions: list[TripPoint]


    def trip_geo_features(trips: Iterable[TripData]) -> list[dict[str, Any]]:
        """Genearte GeoJson Features from trip data."""

        return [
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[p["x"], p["y"]] for p in trip["positions"]],
                },
                "properties": {
                    "times": [ts.isoformat() for ts in trip["timestamps"]],
                    "style": {"color": "blue", "weight": 3, "opacity": 0.6},
                    "icon": "circle",
                    "iconstyle": {
                        "fillColor": "red",
                        "fillOpacity": 0.6,
                        "stroke": "false",
                        "radius": 5,
                    },
                },
            }
            for trip in trips
        ]
    return (trip_geo_features,)


@app.cell
def _(folium, trip_geo_features, trips):
    from folium.plugins import TimestampedGeoJson

    lng, lat = -0.13381370382489707, 51.518898098201326
    features = trip_geo_features(trips.to_dicts())

    # Create a map centered on the first person's location
    m = folium.Map(
        location=[lat, lng],
        zoom_start=13,
        tiles="CartoDB dark_matter",  # Dark theme map
    )

    TimestampedGeoJson(
        {"type": "FeatureCollection", "features": features},
        add_last_point=True,
        loop=True,
        auto_play=False,
        date_options="YYYY-MM-DD HH:mm:ss",
        period="PT1M",  # 5 minutes between points
        duration="PT1M",  # 1 minute display duration for each point
        loop_button=True,
    ).add_to(m)

    m
    return


if __name__ == "__main__":
    app.run()
