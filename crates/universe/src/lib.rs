mod agents;
mod error;
mod idents;
pub mod init;
mod models;
mod simulation;

pub use self::agents::*;
pub use self::simulation::state;
pub use self::simulation::{Entity, Simulatable, Simulation, SimulationBuilder, State};

#[cfg(test)]
mod tests {
    use arrow_cast::pretty::print_columns;
    use geoarrow_geoparquet::GeoParquetRecordBatchReaderBuilder;

    #[test]
    fn test_simulation() {
        let file = std::fs::File::open("../../data/georef-united-states-of-america-county.parquet")
            .unwrap();

        let mut asd = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = asd.next().unwrap().unwrap();
        //let asd = read_geojson(BufReader::new(file), Some(1024)).unwrap();
        let arr = batch.column_by_name("ste_name").unwrap();
        print_columns("counties", &[arr.clone()]).unwrap();
        // print!("{:?};", batch.schema());
    }
}
