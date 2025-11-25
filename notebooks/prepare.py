import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import pyarrow.parquet as pq
    from caspers_universe import (
        load_simulation_setup,
        run_simulation,
        site_routing_graph,
    )

    return (
        Path,
        load_simulation_setup,
        mo,
        pq,
        run_simulation,
        site_routing_graph,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Prepare data to efficiently compute courier routes at runtime

    We want to have a realistic simulation and provide realisitic looking data.
    Especially when plotting movement data it, sticking to roads / pathways make
    visulaizations much more compelling.

    The baseic strategy is as follows.

    1. select a sufficiently large region around our kitchen sites.
    2. Load data from OSM and normalize node / edge information
    3. Store as geoparquet so we can efficiently load it

    This data is used within the rust kernel to create a representation optimized for
    route-finding which allows us to compute all courier trips and generally movements
    along road networks on-the-fly.
    """)
    return


@app.cell
def _(Path, load_simulation_setup):
    setup_path = Path("../crates/universe/templates/base").absolute()
    # load the overall simulation setup to get site configurations.
    setup = load_simulation_setup(setup_path.as_uri())

    # make sure the data paths exist.
    setup_path.joinpath("routing/nodes").mkdir(exist_ok=True, parents=True)
    setup_path.joinpath("routing/edges").mkdir(exist_ok=True, parents=True)
    return (setup,)


@app.cell
def routing_data(pq, setup, site_routing_graph):
    # this cell might error when marimo processes results,
    # the variables will still be assigned

    # load and process open street map data.
    for site in setup.sites:
        nodes, edges = site_routing_graph(site.info)
        pq.write_table(nodes, f"./data/routing/nodes/{site.info.name}.parquet")
        pq.write_table(edges, f"./data/routing/edges/{site.info.name}.parquet")

    print("done")
    return


@app.cell
def run_simulation(Path, run_simulation, setup):
    output_path = Path("./data").absolute()
    routing_path = Path("../data/routing/").absolute()

    run_simulation(setup, 100, str(output_path), str(routing_path))
    return


@app.cell(hide_code=True)
def site_input(mo, setup):
    dropdown = mo.ui.dropdown(
        options={site.info.name: idx for idx, site in enumerate(setup.sites)},
        value="london",
        label="pick a location",
    )
    dropdown
    return (dropdown,)


@app.cell(hide_code=True)
def site_plot(dropdown, setup):
    from caspers_universe import plot_site

    plot_site(setup.sites[dropdown.value].info)
    return


@app.cell
def _(dropdown, setup):
    from lonboard import Map, H3HexagonLayer
    from lonboard.basemap import CartoStyle, MaplibreBasemap
    import h3

    from pyarrow import Table

    # from shapely.geometry import Po

    site = setup.sites[dropdown.value].info

    resolutions = [6, 7, 8, 9, 10]
    cells = [
        h3.latlng_to_cell(site.latitude, site.longitude, res) for res in resolutions
    ]

    table = Table.from_pydict({"h3_index": cells})

    # cell = h3.latlng_to_cell(site.latitude, site.longitude, 9)
    # locations = [h3.cells_to_geo([cell]) for cell in cells]
    # features = [{"type": "Feature", "geometry": geo, "properties": {}} for geo in locations]

    # series = gpd.GeoSeries(location)
    # gdf = gpd.GeoDataFrame.from_features(features)

    # A GeoDataFrame with Polygon or MultiPolygon geometries
    basemap = MaplibreBasemap(mode="interleaved", style=CartoStyle.DarkMatter)
    # layer = PolygonLayer.from_geopandas(
    #    gdf,
    #    get_fill_color=[255, 0, 0],
    #    get_line_color=[0, 100, 100, 150],
    # )
    layer = H3HexagonLayer(
        table, get_hexagon=table["h3_index"], get_fill_color=[255, 0, 0]
    )
    m = Map(layer, basemap=basemap)
    m
    return


if __name__ == "__main__":
    app.run()
