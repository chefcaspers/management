import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pyarrow.parquet as pq
    from caspers_universe import prepare_site, Site
    import polars as pl
    return Site, mo, pq, prepare_site


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
def _(Site):
    sites = [
        Site(
            name="london",
            latitude=51.518898098201326,
            longitude=-0.13381370382489707,
        ),
        Site(
            name="amsterdam",
            latitude=52.3358324410348,
            longitude=4.888889169536197,
        ),
    ]
    return (sites,)


@app.cell
def routing_data(pq, prepare_site, sites):
    # this cell might error when marimo processes results,
    # the variables will still be assigned
    for site in sites:
        nodes, edges = prepare_site(site)
        pq.write_table(nodes, f"./sites/nodes/{site.name}.parquet")
        pq.write_table(edges, f"./sites/edges/{site.name}.parquet")

    print("done")
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
