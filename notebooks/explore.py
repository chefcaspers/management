import marimo

__generated_with = "0.15.2"
app = marimo.App()


@app.cell
def _():
    import h3
    import os
    import polars as pl
    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
    import plotly.express as px
    import folium
    return folium, go, make_subplots, os, pl


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
def _(pl):
    objects = pl.read_parquet("./data/objects/")
    people = pl.read_parquet("./data/population/people")
    positions = pl.read_parquet("./data/population/positions")
    orders = pl.read_parquet("./data//orders")
    order_lines = pl.read_parquet("./data//order_lines")

    orders_counts = pl.sql("SELECT status, count(*) as count FROM orders GROUP BY status").collect()
    order_lines_counts = pl.sql("SELECT status, count(*) as count FROM order_lines GROUP BY status").collect()
    people_counts = pl.sql("SELECT role, count(*) as count FROM people GROUP BY role").collect()
    object_counts = pl.sql("SELECT label, count(*) as count FROM objects GROUP BY label").collect()
    return object_counts, orders, people, people_counts, positions


@app.cell
def _(orders):
    orders.write_delta("data/orders_delta")
    return


@app.cell
def _(people, positions):
    people.write_parquet("./people.parquet")
    positions.write_parquet("./positions.parquet")
    return


@app.cell
def _(go, make_subplots, object_counts, people_counts):
    # Create subplot with 1 row and 2 columns
    fig = make_subplots(rows=1, cols=2, specs=[[{"type": "pie"}, {"type": "pie"}]])

    # Add object counts pie chart
    fig.add_trace(
        go.Pie(
            labels=object_counts['label'].to_list(),
            values=object_counts['count'].to_list(),
            name="Objects",
            title="Object Types",
            showlegend=True,
            legendgroup="objects",
        ),
        row=1, col=1
    )

    # Add people counts pie chart
    fig.add_trace(
        go.Pie(
            labels=people_counts['role'].to_list(),
            values=people_counts['count'].to_list(),
            name="People",
            title="People Roles",
            showlegend=True,
            legendgroup="people",
        ),
        row=1, col=2
    )

    # Update layout
    fig.update_layout(
        title_text="Data Composition Analysis",
        height=500,
        showlegend=True,
        template="plotly_dark",
    )

    fig
    return


@app.cell
def _(people, pl, positions):
    _trips = people.select(['id', 'role']).filter(pl.col('role') == 'courier').join(positions, on='id', how='left').sort(['id', 'timestamp']).group_by('id', maintain_order=True)
    return


@app.cell
def _(people, pl, positions):
    from folium.plugins import TimestampedGeoJson
    import datetime
    epoch = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    _trips = people.select(['id', 'role']).filter(pl.col('role') == 'courier').join(positions, on='id', how='left').sort(['id', 'timestamp']).group_by('id', maintain_order=True)
    routes = []
    _features = []
    for id, trip in _trips:
        _poss = trip['position'].to_list()
        _tss = trip['timestamp'].to_list()
        feature = [{'type': 'Feature', 'geometry': {'type': 'LineString', 'coordinates': _poss[:idx]}, 'properties': {'times': [ts.isoformat() for ts in _tss[:idx]], 'icon': 'circle', 'iconstyle': {'fillColor': 'red', 'fillOpacity': 0.6, 'stroke': 'false', 'radius': 5}}} for idx in range(len(_poss))]
        _features.extend(feature)
        routes.append({'type': 'Feature', 'geometry': {'type': 'LineString', 'coordinates': _poss}, 'properties': {'times': [ts.isoformat() for ts in _tss], 'style': {'color': 'blue', 'weight': 3, 'opacity': 0.6}, 'icon': 'circle', 'iconstyle': {'fillColor': 'red', 'fillOpacity': 0.6, 'stroke': 'false', 'radius': 5}}})
    return TimestampedGeoJson, routes, trip


@app.cell
def _(trip):
    _poss = trip['position'].to_list()
    _tss = trip['timestamp'].to_list()
    _features = [{'type': 'Feature', 'geometry': {'type': 'LineString', 'coordinates': [pos0, pos1]}, 'properties': {'times': [ts0.isoformat(), ts1.isoformat()], 'icon': 'circle', 'iconstyle': {'fillColor': 'red', 'fillOpacity': 0.6, 'stroke': 'false', 'radius': 5}}} for (pos0, pos1), (ts0, ts1) in zip(zip(_poss[:-1], _poss[1:]), zip(_tss[:-1], _tss[1:]))]
    return


@app.cell
def _(TimestampedGeoJson, folium, routes):
    lng, lat = -0.13381370382489707, 51.518898098201326
    resolution = 6

    # Create a map centered on the first person's location
    m = folium.Map(
        location=[lat, lng],
        zoom_start=13,
        tiles="CartoDB dark_matter",  # Dark theme map
    )

    TimestampedGeoJson(
        {"type": "FeatureCollection", "features": routes},
        add_last_point=True,
        loop=True,
        auto_play=False,
        date_options="YYYY/MM/DD HH:mm:ss",
        period="PT1M",  # 5 minutes between points
        duration="PT1M",  # 1 minute display duration for each point
        loop_button=True,
    ).add_to(m)

    m
    return


if __name__ == "__main__":
    app.run()
