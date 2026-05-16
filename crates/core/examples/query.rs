use anyhow::Result;
use datafusion::{arrow::util::pretty::print_batches, prelude::*};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let Some(filename) = std::env::args().nth(1) else {
        return Ok(());
    };
    info!(filename);

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(
            "data/fhvhv_tripdata_2026-02.parquet",
            ParquetReadOptions::default(),
        )
        .await?;

    let schema = df.schema();
    info!(schema = %schema.tree_string());

    let rb = df.limit(1000, Some(3))?.collect().await?;
    let _ = print_batches(&rb);

    Ok(())
}
