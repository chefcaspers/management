import marimo

__generated_with = "0.15.2"
app = marimo.App()


@app.cell
def _():
    import pyarrow.parquet as pq
    import osmnx as ox
    from pint import UnitRegistry
    from copy import deepcopy
    from uuid import uuid5, NAMESPACE_DNS
    from shapely.geometry import LineString, Point
    import pyarrow as pa
    import geoarrow.pyarrow as ga
    import networkx as nx
    return (
        LineString,
        NAMESPACE_DNS,
        Point,
        UnitRegistry,
        deepcopy,
        ga,
        nx,
        ox,
        pa,
        pq,
        uuid5,
    )


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
def _(
    LineString,
    NAMESPACE_DNS,
    Point,
    UnitRegistry,
    deepcopy,
    ga,
    nx,
    pa,
    uuid5,
):
    def process_nodes(location: str, G: nx.MultiDiGraph):
        node_ids = []
        node_coords = []
        node_props = []

        for id, meta in list(G.nodes(data=True)):
            properties = deepcopy(meta)
            node_coords.append(Point(properties["x"], properties["y"]))
            del properties["x"]
            del properties["y"]
            properties["osmid"] = id

            node_ids.append(uuid5(NAMESPACE_DNS, f"osmid/{id}").bytes)
            node_props.append(properties)

        location_arr = pa.array(
            [location] * len(node_ids), pa.dictionary(pa.int32(), pa.string())
        )
        node_ids_arr = pa.array(node_ids, pa.uuid())
        node_props_arr = pa.array(node_props)
        node_coords_arr = ga.as_geoarrow([str(point) for point in node_coords])

        return pa.Table.from_arrays(
            [location_arr, node_ids_arr, node_props_arr, node_coords_arr],
            ["location", "id", "properties", "geometry"],
        )


    def process_edges(location: str, G: nx.MultiDiGraph):
        ureg = UnitRegistry()

        lines = []
        sources = []
        targets = []
        props = []

        for source, target, meta in list(G.edges(data=True)):
            properties = {}

            # we internally store UUIDs for nodes and edges. so we create a static mapping
            # via UUID v5 ans store the original values in the properties.
            source_uuid = uuid5(NAMESPACE_DNS, f"osmid/{source}")
            target_uuid = uuid5(NAMESPACE_DNS, f"osmid/{target}")

            properties["osmid_source"] = source
            properties["osmid_target"] = target

            if "maxspeed" in meta:
                if isinstance(meta["maxspeed"], list):
                    try:
                        properties["maxspeed_m_s"] = (
                            ureg(meta["maxspeed"][0]).to("m/s").magnitude
                        )
                    except Exception:
                        properties["maxspeed_m_s"] = None
                elif isinstance(meta["maxspeed"], str):
                    try:
                        properties["maxspeed_m_s"] = (
                            ureg(meta["maxspeed"]).to("m/s").magnitude
                        )
                    except Exception:
                        properties["maxspeed_m_s"] = None
                else:
                    properties["maxspeed_m_s"] = None
            else:
                properties["maxspeed_m_s"] = None

            if "name" in meta:
                # properties["name"] = meta["name"]
                if isinstance(meta["name"], list):
                    properties["name"] = meta["name"][0]
                else:
                    properties["name"] = meta["name"]
            else:
                properties["name"] = None

            if "geometry" in meta:
                lines.append(meta["geometry"])
            else:
                # when there is no geometry, we have a street with no additional
                # inner nodes. i.e. a straight line.
                source_tuple = G.nodes[source]["x"], G.nodes[source]["y"]
                target_tuple = G.nodes[target]["x"], G.nodes[target]["y"]
                lines.append(LineString([source_tuple, target_tuple]))

            if isinstance(meta["highway"], list):
                properties["highway"] = meta["highway"][0]
            else:
                properties["highway"] = meta["highway"]

            properties["length"] = meta["length"] or 10.0
            # properties["highway"] = meta["highway"]
            # properties["access"] = meta.get("access")
            # properties["oneway"] = meta["oneway"]

            props.append(properties)
            sources.append(source_uuid.bytes)
            targets.append(target_uuid.bytes)

        location_arr = pa.array(
            [location] * len(sources), pa.dictionary(pa.int32(), pa.string())
        )
        source_array = pa.array(sources, pa.uuid())
        target_array = pa.array(targets, pa.uuid())
        props_array = pa.array(props)
        geo_lines = ga.as_geoarrow([str(line) for line in lines])

        return pa.Table.from_arrays(
            [location_arr, source_array, target_array, props_array, geo_lines],
            ["location", "source", "target", "properties", "geometry"],
        )
    return process_edges, process_nodes


@app.cell
def _():
    location_name = "london"
    lng, lat = -0.13381370382489707, 51.518898098201326
    resolution = 6

    location_name = "amsterdam"
    lng, lat = 4.888889169536197, 52.3358324410348
    resolution = 6
    return lat, lng


@app.cell
def _(lat, lng, ox):
    G = ox.graph_from_point(
        (lat, lng),
        dist=3000,
        network_type="bike",
    )
    # Gp = ox.projection.project_graph(G)
    return (G,)


@app.cell
def _(lat, lng):
    import folium
    import h3
    import polars as pl

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


@app.cell
def _(G, nx, pq, process_edges, process_nodes):
    def process(location: str, G: nx.MultiDiGraph):
        nodes = process_nodes(location, G)
        print(nodes.schema)
        pq.write_table(nodes, f"./sites/{location}/nodes.parquet")

        edges = process_edges(location, G)
        print(edges.schema)
        pq.write_table(edges, f"./sites/{location}/edges.parquet")


    # process("london", G)
    process("amsterdam", G)
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
