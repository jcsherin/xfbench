use std::{env, fs};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Deserialize)]
struct Config {
    tables: Vec<TableSpec>,
}

#[expect(dead_code)]
#[derive(Debug, Deserialize)]
struct TableSpec {
    name: String,
    format: Format,
    location: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Format {
    Parquet,
    Vortex,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = env::args()
        .nth(1)
        .with_context(|| "usage: query -- /path/to/config")?;
    info!(config_path = %config_path, "reading config");

    let config: Config = toml::from_str(&fs::read_to_string(&config_path)?)?;
    info!(tables = ?config.tables.len(), "parsed config");

    eprintln!("{config:#?}");

    Ok(())
}
