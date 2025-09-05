import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pyarrow.parquet as pq
    from caspers_universe.prepare import prepare_location
    import polars as pl
    import pyarrow as pa
    import geoarrow.pyarrow as ga
    return mo, pl, pq, prepare_location


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Prepare data to efficiently compute courier routes at runtime

    We want to have a realistic simuation and provide realisitic looking data. Especially when plotting movement data it, sticking to roads / pathways make visulaizations much more compelling.
    The baseic strategy is as follows.

    1. select a sufficiently large region around our kitchen sites.
    2. Load data from OSM and normalive node / edge information
    3. Store as geoparquet so we can efficiently load it

    Within the rust kernel, we can further prepare ourselves by creating a graph representation optimized for shortest path search.
    This allows us to compute shortest path on every iteration.
    """
    )
    return


@app.cell
def _():
    location_name = "london"
    lng, lat = -0.13381370382489707, 51.518898098201326
    resolution = 6

    location_name = "amsterdam"
    lng, lat = 4.888889169536197, 52.3358324410348
    resolution = 6
    return lat, lng, location_name


@app.cell
def _(lat, lng, location_name, pq, prepare_location):
    nodes, edges = prepare_location(location_name, lat, lng)

    pq.write_table(nodes, f"./sites/nodes/{location_name}.parquet")
    pq.write_table(edges, f"./sites/edges/{location_name}.parquet")

    print("done")
    # this cell might error when marimo processes results,
    # the variables will still be assigned
    return


@app.cell
def _(pq):
    table_nodes = pq.read_table("./sites/nodes/")
    table_edges = pq.read_table("./sites/edges/")
    return table_edges, table_nodes


@app.cell
def _(table_nodes):
    table_nodes.schema
    return


@app.cell
def _(table_edges):
    table_edges.schema
    return


@app.cell
def _(lat, lng, pl):
    import folium
    import h3


    plot_people = False
    population = pl.read_parquet("./data/population/1745768936.parquet")

    m = folium.Map(location=[lat, lng], zoom_start=13, tiles="CartoDB dark_matter")

    cell = h3.latlng_to_cell(lat, lng, 9)
    location = h3.cells_to_geo([cell])
    folium.GeoJson(location).add_to(m)

    # cells = h3.grid_disk(cell, 1)
    # pickup_area = h3.cells_to_geo(cells)
    # folium.GeoJson(pickup_area).add_to(m)

    cells = h3.grid_disk(cell, 10)
    delivery_area = h3.cells_to_geo(cells)
    folium.GeoJson(delivery_area).add_to(m)

    folium.Marker(
        location=[lat, lng],
        popup="Chef Casper's Kitchen",
        icon=folium.Icon(color="red", icon="kitchen-set", prefix="fa"),
    ).add_to(m)

    if plot_people:
        for row in population.select(["position", "role"]).rows():
            folium.Marker(
                location=[row[0][1], row[0][0]],
                popup="Person",
                icon=folium.Icon(
                    color="blue" if row[1] == "customer" else "red",
                    icon="user",
                    prefix="fa",
                ),
            ).add_to(m)

    m
    return


if __name__ == "__main__":
    app.run()
