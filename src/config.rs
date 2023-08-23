use std::{env::current_dir, net::IpAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::de::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
pub struct Config {
  pub server: ServerConfig,
  pub frontend_mgr: FrontendMgrConfig,
  pub backend_mgr: BackendMgrConfig,
  pub service_mgr: ServiceMgrConfig,
  pub db: DbConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
  pub http_port: u32,
  pub https_port: u32,
  pub backlog: u32,
  #[serde(deserialize_with = "deserialize_keep_alive", default)]
  pub keep_alive: Option<Duration>,
  pub max_connection_rate: usize,
  pub max_connections: usize,
  pub workers: usize,
  pub max_frame_size: usize,
}

fn deserialize_keep_alive<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where D: Deserializer<'de> {
  let keep_alive: u64 = Deserialize::deserialize(deserializer)?;
  if keep_alive == 0 {
    Ok(None)
  } else {
    Ok(Some(Duration::from_secs(keep_alive)))
  }
}

#[derive(Debug, Deserialize)]
pub struct FrontendMgrConfig {
  pub frontends: Vec<FrontendConfig>,
}

#[derive(Debug, Deserialize)]
pub struct BackendMgrConfig {
  pub backends: Vec<BackendConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ServiceMgrConfig {
  pub stale_threshold: u32,
  pub unhealthy_threshold: u32,
}

#[derive(Debug, Deserialize)]
pub struct FrontendConfig {
  pub domain: String,
  pub http_port: u32,
  pub https_port: u32,
  pub public_ip: IpAddr,
  pub private_ip: IpAddr,
}

#[derive(Debug, Deserialize)]
pub struct BackendConfig {
  pub http_port: u32,
  pub private_ip: IpAddr,
}

#[derive(Debug, Deserialize)]
pub struct DbConfig {
  #[serde(deserialize_with = "deserialize_path")]
  pub path: String,
  pub seriesdb: SeriesdbConfig,
}

#[derive(Debug, Deserialize)]
pub struct SeriesdbConfig {
  pub table_cache_num_shard_bits: i32,
  pub write_buffer_size: usize,
  pub max_write_buffer_number: i32,
  pub min_write_buffer_number_to_merge: i32,
  pub max_bytes_for_level_base: u64,
  pub max_bytes_for_level_multiplier: f64,
  pub target_file_size_base: u64,
  pub target_file_size_multiplier: i32,
  pub level_zero_file_num_compaction_trigger: i32,
  pub max_background_jobs: i32,
}

fn deserialize_path<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
  let path: String = Deserialize::deserialize(deserializer)?;
  let path = PathBuf::from(path);
  if path.is_absolute() {
    Ok(path.display().to_string())
  } else {
    current_dir()
      .with_context(|| format!("Failed to get current dir"))
      .map_err(serde::de::Error::custom)?
      .join(path)
      .display()
      .to_string()
      .parse()
      .map_err(serde::de::Error::custom)
  }
}

impl Config {
  pub(crate) fn new(path: &str) -> Result<Self> {
    Ok(
      config::Config::builder()
        .add_source(config::File::with_name(path))
        .build()
        .with_context(|| format!("Failed to read config from: {:?}", path))?
        .try_deserialize()
        .with_context(|| format!("Failed to deserialize config from: {:?}", path))?,
    )
  }
}

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new("config/config.toml").unwrap());
