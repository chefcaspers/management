from copy import deepcopy
from typing import Literal
from uuid import NAMESPACE_DNS, uuid5

import geoarrow.pyarrow as ga
import networkx as nx
import osmnx as ox
import pyarrow as pa
from pint import UnitRegistry
from shapely.geometry import LineString, Point

_EDGE_SCHEMA = pa.schema(
    fields=[
        pa.field("location", pa.dictionary(pa.int32(), pa.string())),
        pa.field("source", pa.uuid()),
        pa.field("target", pa.uuid()),
        pa.field(
            "properties",
            pa.struct(
                fields=[
                    pa.field("highway", pa.string()),
                    pa.field("length", pa.float64()),
                    pa.field("maxspeed_m_s", pa.float64()),
                    pa.field("name", pa.string()),
                    pa.field("osmid_source", pa.int64()),
                    pa.field("osmid_target", pa.int64()),
                ]
            ),
        ),
        pa.field("geometry", ga.linestring()),
    ]
)

_NODE_SCHEMA = pa.schema(
    fields=[
        pa.field("location", pa.dictionary(pa.int32(), pa.string())),
        pa.field("id", pa.uuid()),
        pa.field(
            "properties",
            pa.struct(
                fields=[
                    pa.field("highway", pa.string()),
                    pa.field("osmid", pa.int64()),
                    pa.field("railway", pa.string()),
                    pa.field("ref", pa.string()),
                    pa.field("street_count", pa.int64()),
                ]
            ),
        ),
        pa.field("geometry", ga.point()),
    ]
)


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
        arrays=[location_arr, node_ids_arr, node_props_arr, node_coords_arr],
        schema=_NODE_SCHEMA,
    )


def process_edges(location: str, G: nx.MultiDiGraph) -> pa.Table:
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
        arrays=[location_arr, source_array, target_array, props_array, geo_lines],
        schema=_EDGE_SCHEMA,
    )


def prepare_location(
    location: str,
    lat: float,
    lng: float,
    network_type: Literal["bike", "drive", "walk"] = "bike",
    distance: int = 3000,
) -> tuple[pa.Table, pa.Table]:
    """Prepare location data for Caspers Universe.

    This downloads the street / path network around the given location
    and processes it for use in route planning within the simulation.

    Args:
        location (str): The location name.
        lat (float): The latitude of the location.
        lng (float): The longitude of the location.
        network_type (Literal["bike", "drive", "walk"], optional): The type of network to use. Defaults to "bike".
        distance (int, optional): The radius around the location to use for the network. Defaults to 3000.

    Returns:
        tuple[pa.Table, pa.Table]: The nodes and edges tables.
    """
    neighbourhood = ox.graph_from_point(
        (lat, lng),
        dist=distance,
        network_type=network_type,
    )
    nodes = process_nodes(location, neighbourhood)
    edges = process_edges(location, neighbourhood)

    return nodes, edges
