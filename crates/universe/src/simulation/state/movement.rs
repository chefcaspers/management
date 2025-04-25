use std::collections::{HashMap, VecDeque};
use std::sync::LazyLock;

use arrow_array::cast::AsArray as _;
use arrow_array::{RecordBatch, types::Float64Type};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::SchemaExt;
use fast_paths::{FastGraph, InputGraph, PathCalculator};
use geo::Point;
use geo_traits::to_geo::ToGeoLineString;
use geoarrow::array::{LineStringArray, PointArray};
use geoarrow::scalar::{LineString as ArrowLineString, Point as ArrowPoint};
use geoarrow::trait_::ArrayAccessor as _;
use geoarrow_schema::{CoordType, Dimension, LineStringType, PointType};
use h3o::LatLng;
use indexmap::IndexSet;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Transport {
    Foot,
    Bicycle,
    Car,
    Bus,
    Train,
    Plane,
    Ship,
}

impl Transport {
    /// Returns the default velocity of the transport in km/h.
    fn default_velocity_km_h(&self) -> f64 {
        match self {
            Transport::Foot => 5.0,
            Transport::Bicycle => 15.0,
            Transport::Car => 60.0,
            Transport::Bus => 30.0,
            Transport::Train => 100.0,
            Transport::Plane => 800.0,
            Transport::Ship => 20.0,
        }
    }

    fn default_velocity_m_s(&self) -> f64 {
        self.default_velocity_km_h() / 3.6
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JourneyLeg {
    destination: Point,
    distance_m: usize,
}

impl<T: Into<Point>> From<(T, usize)> for JourneyLeg {
    fn from(value: (T, usize)) -> Self {
        JourneyLeg {
            destination: value.0.into(),
            distance_m: value.1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Journey {
    legs: VecDeque<JourneyLeg>,
}

impl Journey {
    pub fn distance_m(&self) -> usize {
        self.legs.iter().map(|leg| leg.distance_m).sum()
    }

    pub fn advance(&mut self, transport: Transport, time_step: std::time::Duration) {
        let velocity_m_s = transport.default_velocity_m_s();
        let distance_m = velocity_m_s * time_step.as_secs_f64();
        let mut distance_remaining = distance_m;
        while distance_remaining > 0. {
            let leg = self.legs.pop_front().unwrap();
            if leg.distance_m as f64 <= distance_remaining {
                distance_remaining -= leg.distance_m as f64;
            } else {
                self.legs.push_front(JourneyLeg {
                    destination: leg.destination,
                    distance_m: leg.distance_m - distance_remaining.round() as usize,
                });
            }
        }
    }
}

impl<T: Into<JourneyLeg>> FromIterator<T> for Journey {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Journey {
            legs: iter.into_iter().map(Into::into).collect(),
        }
    }
}

pub struct TripPlanner {
    routing: RoutingData,
    graph: FastGraph,
    router: PathCalculator,
}

impl TripPlanner {
    fn new(routing: RoutingData) -> Self {
        let graph = routing.build_router();
        let router = fast_paths::create_calculator(&graph);
        Self {
            routing,
            graph,
            router,
        }
    }

    pub fn plan(
        &mut self,
        origin: impl AsRef<Uuid>,
        destination: impl AsRef<Uuid>,
    ) -> Option<Journey> {
        let origin_id = self.routing.node_map.get_index_of(origin.as_ref())?;
        let destination_id = self.routing.node_map.get_index_of(destination.as_ref())?;
        let path = self
            .router
            .calc_path(&self.graph, origin_id, destination_id)?;
        Some(
            path.get_nodes()
                .iter()
                .tuple_windows()
                .flat_map(|(a, b)| {
                    let edge = self.routing.edge_map.get(&(*a, *b)).unwrap();
                    let edge = self.routing.edge(*edge);
                    let legs = edge
                        .geometry()
                        .to_line_string()
                        .points()
                        .tuple_windows()
                        .map(|(p0, p1)| {
                            let distance = LatLng::new(p0.y(), p0.x())
                                .unwrap()
                                .distance_m(LatLng::new(p1.y(), p1.x()).unwrap());
                            JourneyLeg {
                                destination: p1,
                                distance_m: distance.round().abs() as usize,
                            }
                        })
                        .collect::<Vec<_>>();
                    legs.into_iter()
                })
                .collect(),
        )
    }
}

impl From<RoutingData> for TripPlanner {
    fn from(routing: RoutingData) -> Self {
        Self::new(routing)
    }
}

pub struct RoutingData {
    nodes: RecordBatch,
    node_positions: PointArray,
    edges: RecordBatch,
    edge_positions: LineStringArray,
    node_map: IndexSet<Uuid>,
    edge_map: HashMap<(usize, usize), usize>,
}

impl RoutingData {
    fn try_new(nodes: RecordBatch, edges: RecordBatch) -> Result<Self> {
        Self::nodes_schema().logically_equivalent_names_and_types(nodes.schema().as_ref())?;
        Self::edges_schema().logically_equivalent_names_and_types(edges.schema().as_ref())?;
        let mut node_map = IndexSet::new();

        let ids = nodes.column(1).as_fixed_size_binary();
        for id in ids.iter() {
            if let Some(id) = id {
                let id = Uuid::from_slice(id).unwrap();
                node_map.insert(id);
            }
        }

        let mut edge_map = HashMap::new();
        let sources = edges.column(1).as_fixed_size_binary();
        let targets = edges.column(2).as_fixed_size_binary();
        for (index, (source, target)) in sources.iter().zip(targets.iter()).enumerate() {
            if let (Some(source), Some(target)) = (source, target) {
                let source = Uuid::from_slice(source).unwrap();
                let target = Uuid::from_slice(target).unwrap();
                let source_index = node_map.get_index_of(&source).unwrap();
                let target_index = node_map.get_index_of(&target).unwrap();
                edge_map.insert((source_index, target_index), index);
            }
        }

        let node_positions = (nodes.column(3).as_struct(), Dimension::XY).try_into()?;
        let edge_positions = (edges.column(4).as_list::<i32>(), Dimension::XY).try_into()?;

        Ok(Self {
            nodes: nodes.project(&[0, 1, 2])?,
            node_positions,
            edges: edges.project(&[0, 1, 2, 3])?,
            edge_positions,
            node_map,
            edge_map,
        })
    }

    fn nodes_schema() -> SchemaRef {
        use arrow_schema::extension::Uuid as UuidExtension;
        static NODE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            SchemaRef::new(Schema::new(vec![
                Field::new("location", DataType::Utf8, true),
                Field::new("id", DataType::FixedSizeBinary(16), true)
                    .with_extension_type(UuidExtension),
                Field::new(
                    "properties",
                    DataType::Struct(
                        vec![
                            Field::new("highway", DataType::Utf8, true),
                            Field::new("junction", DataType::Utf8, true),
                            Field::new("osmid", DataType::Int64, true),
                            Field::new("railway", DataType::Utf8, true),
                            Field::new("ref", DataType::Utf8, true),
                            Field::new("street_count", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Field::new(
                    "geometry",
                    DataType::Struct(
                        vec![
                            Field::new("x", DataType::Float64, true),
                            Field::new("y", DataType::Float64, true),
                        ]
                        .into(),
                    ),
                    true,
                )
                .with_extension_type(PointType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    Default::default(),
                )),
            ]))
        });
        NODE_SCHEMA.clone()
    }

    fn edges_schema() -> SchemaRef {
        static EDGE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            SchemaRef::new(Schema::new(vec![
                Field::new("location", DataType::Utf8, false),
                Field::new("source", DataType::FixedSizeBinary(16), false),
                Field::new("target", DataType::FixedSizeBinary(16), false),
                Field::new(
                    "properties",
                    DataType::Struct(
                        vec![
                            Field::new("highway", DataType::Utf8, true),
                            Field::new("length", DataType::Float64, true),
                            Field::new("maxspeed_m_s", DataType::Float64, true),
                            Field::new("name", DataType::Utf8, true),
                            Field::new("osmid_source", DataType::Int64, true),
                            Field::new("osmid_target", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                ),
                Field::new_list(
                    "geometry",
                    Field::new_list_field(
                        DataType::Struct(
                            vec![
                                Field::new("x", DataType::Float64, true),
                                Field::new("y", DataType::Float64, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                    true,
                )
                .with_extension_type(LineStringType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    Default::default(),
                )),
            ]))
        });
        EDGE_SCHEMA.clone()
    }

    pub fn nodes(&self) -> impl ExactSizeIterator<Item = StreetNode<'_>> {
        (0..self.nodes.num_rows())
            .into_iter()
            .map(|i| StreetNode::new(self, i))
    }

    fn node(&self, index: usize) -> StreetNode<'_> {
        StreetNode::new(self, index)
    }

    pub fn edges(&self) -> impl ExactSizeIterator<Item = StreetEdge<'_>> {
        (0..self.edges.num_rows())
            .into_iter()
            .map(|i| StreetEdge::new(self, i))
    }

    fn edge(&self, index: usize) -> StreetEdge<'_> {
        StreetEdge::new(self, index)
    }

    fn build_router(&self) -> FastGraph {
        let mut graph = InputGraph::new();

        for edge in self.edges() {
            let source_id = self
                .node_map
                .get_index_of(&Uuid::from_slice(edge.source()).unwrap())
                .unwrap();
            let target_id = self
                .node_map
                .get_index_of(&Uuid::from_slice(edge.target()).unwrap())
                .unwrap();
            graph.add_edge(source_id, target_id, edge.length().round().abs() as usize);
        }

        graph.freeze();
        fast_paths::prepare(&graph)
    }

    pub fn into_trip_planner(self) -> TripPlanner {
        TripPlanner::new(self)
    }
}

pub struct StreetNode<'a> {
    data: &'a RoutingData,
    valid_index: usize,
}

impl<'a> StreetNode<'a> {
    fn new(data: &'a RoutingData, valid_index: usize) -> Self {
        Self { data, valid_index }
    }

    pub fn geometry(&self) -> ArrowPoint<'_> {
        self.data.node_positions.value(self.valid_index)
    }
}

pub struct StreetEdge<'a> {
    data: &'a RoutingData,
    valid_index: usize,
}

impl<'a> StreetEdge<'a> {
    fn new(data: &'a RoutingData, valid_index: usize) -> Self {
        Self { data, valid_index }
    }

    pub fn source(&self) -> &[u8] {
        self.data
            .edges
            .column(1)
            .as_fixed_size_binary()
            .value(self.valid_index)
    }

    pub fn target(&self) -> &[u8] {
        self.data
            .edges
            .column(2)
            .as_fixed_size_binary()
            .value(self.valid_index)
    }

    pub fn length(&self) -> f64 {
        self.data
            .edges
            .column(3)
            .as_struct()
            .column(1)
            .as_primitive::<Float64Type>()
            .value(self.valid_index)
    }

    pub fn geometry(&self) -> ArrowLineString<'_> {
        self.data.edge_positions.value(self.valid_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::concat_batches;
    use arrow_array::RecordBatchReader;
    use geoarrow_geoparquet::GeoParquetRecordBatchReaderBuilder;
    use itertools::Itertools;

    #[test_log::test]
    fn test_osm_nodes() {
        let file = std::fs::File::open("../../notebooks/nodes.parquet").unwrap();
        let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let schema = reader.schema();
        let batches: Vec<_> = reader.into_iter().try_collect().unwrap();
        let nodes = concat_batches(&schema, &batches).unwrap();

        println!("nodes: {:?}", nodes.num_rows());
    }

    #[test_log::test]
    fn test_osm_node_properties() {
        let file = std::fs::File::open("../../notebooks/edges.parquet").unwrap();
        let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let schema = reader.schema();
        let batches: Vec<_> = reader.into_iter().try_collect().unwrap();
        let edges = concat_batches(&schema, &batches).unwrap();

        let file = std::fs::File::open("../../notebooks/nodes.parquet").unwrap();
        let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let schema = reader.schema();
        let batches: Vec<_> = reader.into_iter().try_collect().unwrap();
        let nodes = concat_batches(&schema, &batches).unwrap();

        let routing = RoutingData::try_new(
            nodes, //.project(&[0, 1, 2]).unwrap(),
            edges, //.project(&[0, 1, 2, 3]).unwrap(),
        )
        .unwrap();

        let mut planner = routing.into_trip_planner();
        let ids = planner.routing.nodes.column(1).as_fixed_size_binary();

        let journey = planner
            .plan(
                Uuid::from_slice(ids.value(1)).unwrap(),
                Uuid::from_slice(ids.value(10)).unwrap(),
            )
            .unwrap();

        println!("journey: {:#?}", journey);

        //let (fast_graph, _) = process_road_network(&nodes, &edges);
        //
        //let mut file = std::fs::File::create("../../notebooks/london.bin").unwrap();
        //let serialized = bincode::serde::encode_into_std_write(
        //    &fast_graph,
        //    &mut file,
        //    bincode::config::standard(),
        //)
        //.unwrap();
        //
        //println!("serialized: {:?}", serialized);

        //let deserialized: FastGraph =
        //    bincode::serde::decode_from_slice(&serialized, bincode::config::standard())
        //        .unwrap()
        //        .0;
    }

    #[test_log::test]
    fn test_osm_node_properties2() {
        let mut file = std::fs::File::open("../../notebooks/london.bin").unwrap();

        let fast_graph: FastGraph =
            bincode::serde::decode_from_std_read(&mut file, bincode::config::standard()).unwrap();

        let shortest_path = fast_paths::calc_path(&fast_graph, 8, 6);

        match shortest_path {
            Some(p) => {
                // the weight of the shortest path
                let weight = p.get_weight();

                // all nodes of the shortest path (including source and target)
                let nodes = p.get_nodes();

                println!("nodes: {:?}", nodes);
                println!("weight: {:?}", weight);
            }
            None => {
                // no path has been found (nodes are not connected in this graph)
            }
        }
    }
}
