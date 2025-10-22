import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pyarrow.parquet as pq
    from caspers_universe import prepare_site, Site, load_simulation_setup, run_simulation
    import polars as pl
    from pathlib import Path
    return Path, load_simulation_setup, mo, pq, prepare_site, run_simulation


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Prepare data to efficiently compute courier routes at runtime

    We want to have a realistic simuation and provide realisitic looking data.
    Especially when plotting movement data it, sticking to roads / pathways make
    visulaizations much more compelling.

    The baseic strategy is as follows.

    1. select a sufficiently large region around our kitchen sites.
    2. Load data from OSM and normalize node / edge information
    3. Store as geoparquet so we can efficiently load it

    This data is used within the rust kernel to create a representation optimized for
    route-finding which allows us to compute all courier trips and generally movements
    along road networks on-the-fly.
    """
    )
    return


@app.cell
def _(Path, load_simulation_setup):
    setup_path = Path("../data").absolute()
    # load the overall simulation setup to get site configurations.
    setup = load_simulation_setup(setup_path.as_uri())

    # make sure the data paths exist.
    setup_path.joinpath("routing/nodes").mkdir(exist_ok=True, parents=True)
    setup_path.joinpath("routing/edges").mkdir(exist_ok=True, parents=True)
    return (setup,)


@app.cell
def routing_data(pq, prepare_site, setup):
    # this cell might error when marimo processes results,
    # the variables will still be assigned

    # load and process open street map data.
    for site in setup.sites:
        nodes, edges = prepare_site(site.info)
        pq.write_table(nodes, f"../data/routing/nodes/{site.info.name}.parquet")
        pq.write_table(edges, f"../data/routing/edges/{site.info.name}.parquet")

    print("done")
    return


@app.cell
def run_simulation(Path, run_simulation, setup):
    output_path = Path("./data").absolute()
    routing_path = Path("../data/routing/").absolute()

    run_simulation(setup, 100, str(output_path), str(routing_path))
    return


@app.cell(hide_code=True)
def site_input(mo):
    dropdown = mo.ui.dropdown(
        options={"london": 0, "amsterdam": 1},
        value="london",
        label="pick a location",
    )
    dropdown
    return (dropdown,)


@app.cell(hide_code=True)
def site_plot(dropdown, sites):
    from caspers_universe import plot_site

    plot_site(sites[dropdown.value])

    # population = pl.read_parquet("./data/population/1745768936.parquet")
    # if plot_people:
    #    for row in population.select(["position", "role"]).rows():
    #         folium.Marker(
    #             location=[row[0][1], row[0][0]],
    #             popup="Person",
    #             icon=folium.Icon(
    #                 color="blue" if row[1] == "customer" else "red",
    #                 icon="user",
    #                 prefix="fa",
    #             ),
    #         ).add_to(m)
    return


if __name__ == "__main__":
    app.run()
