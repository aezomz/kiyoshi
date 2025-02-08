use anyhow::{anyhow, Context, Result};
use log::debug;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database_config: DatabaseConfig,
    pub cleanup_tasks: Vec<CleanupTask>,
    pub cron_schedule: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CleanupTask {
    pub name: String,
    #[allow(dead_code)]
    pub description: String,
    pub enabled: bool,
    pub template_query: String,
    pub parameters: HashMap<String, String>,
    pub batch_size: u32,
    pub retry_attempts: u32,
    pub retry_delay_seconds: u32,
    #[serde(default)]
    pub query_interval_seconds: u32,
}

impl Config {
    pub fn load_from_path(path: &str) -> Result<Self> {
        let config_str = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let config_str = substitute_env_vars(&config_str);
        let mut config: Config = serde_yaml::from_str(&config_str)
            .with_context(|| "Failed to parse YAML configuration")?;

        // Validate configuration
        config.validate()?;

        debug!("Configuration loaded and validated successfully");
        Ok(config)
    }

    fn validate(&mut self) -> Result<()> {
        // Validate database configuration
        if self.database_config.host.is_empty() {
            return Err(anyhow!("Database host cannot be empty"));
        }
        if self.database_config.username.is_empty() {
            return Err(anyhow!("Database username cannot be empty"));
        }
        if self.database_config.database.is_empty() {
            return Err(anyhow!("Database name cannot be empty"));
        }

        // Validate cleanup tasks
        if self.cleanup_tasks.is_empty() {
            return Err(anyhow!("No cleanup tasks defined in configuration"));
        }

        if self.cron_schedule.is_empty() {
            return Err(anyhow!("Cron schedule cannot be empty"));
        } else if self.cron_schedule.split_whitespace().count() == 5 {
            self.cron_schedule = ["*", &self.cron_schedule].join(" ");
        }

        for task in &self.cleanup_tasks {
            if task.name.is_empty() {
                return Err(anyhow!("Task name cannot be empty"));
            }
            if task.template_query.is_empty() {
                return Err(anyhow!(
                    "SQL template cannot be empty for task: {}",
                    task.name
                ));
            }
            if task.retry_attempts == 0 {
                return Err(anyhow!(
                    "Retry attempts must be greater than 0 for task: {}",
                    task.name
                ));
            }
            if task.batch_size == 0 {
                return Err(anyhow!(
                    "Batch size must be greater than 0 for task: {}",
                    task.name
                ));
            }
        }

        Ok(())
    }
}

pub fn substitute_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    // Simple environment variable substitution
    // Format: ${VAR_NAME:-default_value} or ${VAR_NAME}
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let var_spec = &result[start + 2..start + end];
            let (var_name, default_value) = if let Some(idx) = var_spec.find(":-") {
                (&var_spec[..idx], Some(&var_spec[idx + 2..]))
            } else {
                (var_spec, None)
            };

            let value = std::env::var(var_name)
                .ok()
                .or_else(|| default_value.map(|s| s.to_string()))
                .unwrap_or_default();

            debug!(
                "Substituting environment variable: {}={}",
                var_name,
                if var_name.contains("password") {
                    "[REDACTED]"
                } else {
                    &value
                }
            );
            result.replace_range(start..start + end + 1, &value);
        } else {
            break;
        }
    }
    result
}
