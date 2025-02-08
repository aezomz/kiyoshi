use super::config::DatabaseConfig;
use anyhow::{anyhow, Result};
use log::debug;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

pub struct Database {
    pool: MySqlPool,
}

impl Database {
    pub async fn new(config: &DatabaseConfig) -> Result<Self, anyhow::Error> {
        // Validate required configuration values
        if config.password.is_empty() {
            return Err(anyhow!("Database password is required but not provided"));
        }

        // Log database connection details (excluding sensitive info)
        debug!(
            "Connecting to database: host={}, port={}, user={}, database={}",
            config.host, config.port, config.username, config.database
        );

        let connection_string = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );

        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&connection_string)
            .await;

        match pool {
            Ok(pool) => Ok(Self { pool }),
            Err(e) => Err(anyhow!("Failed to connect to database: {}", e)),
        }
    }

    pub async fn execute_query(&self, query: &str) -> Result<(u64, f64)> {
        let start = std::time::Instant::now();
        let result = sqlx::query(query).execute(&self.pool).await;
        let elapsed = start.elapsed().as_secs_f64();

        match result {
            Ok(result) => Ok((result.rows_affected(), elapsed)),
            Err(e) => Err(anyhow!("Database query failed: {:?}", e)),
        }
    }
}
