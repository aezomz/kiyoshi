use anyhow::{anyhow, Context, Result};
use log::{debug, info, warn};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct FullConfig {
    pub config: Config,
    pub cleanup_tasks: Vec<CleanupTask>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database_config: DatabaseConfig,
    pub slack_config: SlackConfig,
    pub safe_mode: SafeMode,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_config: DatabaseConfig {
                host: String::from("localhost"),
                port: 3306,
                username: String::from("root"),
                password: String::from("123"),
                database: String::from("my_database"),
            },
            slack_config: SlackConfig {
                bot_token: String::from("xoxb-1234567890"),
                channel_id: String::from("C01234567890"),
                enabled: true,
            },
            safe_mode: SafeMode {
                enabled: true,
                retention_days: 30,
            },
        }
    }
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
pub struct SlackConfig {
    pub bot_token: String,
    pub channel_id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone)]
pub struct CleanupTask {
    pub name: String,
    #[allow(dead_code)]
    pub description: String,
    pub cron_schedule: String,
    pub enabled: bool,
    pub template_query: String,
    pub parameters: HashMap<String, String>,
    pub batch_size: u32,
    pub retry_attempts: u32,
    pub retry_delay_seconds: u32,
    #[serde(default)]
    pub query_interval_seconds: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SafeMode {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub retention_days: u64,
}

impl FullConfig {
    pub fn load_from_path(path: &str) -> Result<Self> {
        let config_str = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let config_str = substitute_env_vars(&config_str);
        let mut config: FullConfig = serde_yaml::from_str(&config_str)
            .with_context(|| "Failed to parse YAML configuration")?;

        // Validate configuration
        config.validate()?;

        debug!("Configuration loaded and validated successfully");
        Ok(config)
    }

    fn validate(&mut self) -> Result<()> {
        // Validate database configuration
        if self.config.database_config.host.is_empty() {
            return Err(anyhow!("Database host cannot be empty"));
        }
        if self.config.database_config.username.is_empty() {
            return Err(anyhow!("Database username cannot be empty"));
        }
        if self.config.database_config.database.is_empty() {
            return Err(anyhow!("Database name cannot be empty"));
        }

        // Validate cleanup tasks
        if self.cleanup_tasks.is_empty() {
            return Err(anyhow!("No cleanup tasks defined in configuration"));
        }

        for task in &mut self.cleanup_tasks {
            if task.name.is_empty() {
                return Err(anyhow!("Task name cannot be empty"));
            }
            if task.cron_schedule.is_empty() {
                return Err(anyhow!("Cron schedule cannot be empty"));
            } else if task.cron_schedule.split_whitespace().count() == 5 {
                task.cron_schedule = ["0", &task.cron_schedule].join(" ");
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
                if var_name.to_lowercase().contains("password")
                    || var_name.to_lowercase().contains("bot_token")
                {
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

pub fn load_env_from_file(env_file_path: &str) -> Result<()> {
    let env_content = std::fs::read_to_string(env_file_path)
        .with_context(|| format!("Failed to read env file '{}'", env_file_path))?;

    let trimmed_content = env_content.trim();
    if trimmed_content.is_empty() {
        warn!(
            "Environment file '{}' is empty, skipping environment variable loading",
            env_file_path
        );
        return Ok(());
    }

    let env_vars: HashMap<String, serde_json::Value> = serde_json::from_str(trimmed_content)
        .with_context(|| {
            let preview = if trimmed_content.len() > 500 {
                format!("{}...", &trimmed_content[..500])
            } else {
                trimmed_content.to_string()
            };
            format!(
                "Failed to parse env file '{}' as JSON. File content (first 500 chars): {}",
                env_file_path, preview
            )
        })?;

    let count = env_vars.len();
    for (key, value) in env_vars {
        let string_value = match value {
            serde_json::Value::String(s) => s,
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => String::new(),
            _ => {
                warn!(
                    "Unsupported value type for environment variable '{}', converting to string representation",
                    key
                );
                value.to_string()
            }
        };
        std::env::set_var(key, string_value);
    }

    info!(
        "Loaded {} environment variables from '{}'",
        count, env_file_path
    );

    Ok(())
}
