use anyhow::Result;
use log::{info, warn};
use serde_json;
use slack_api_client::{CreateMessage, SlackClient};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};

use crate::{
    cleaner::{
        config::{CleanupTask, Config},
        db::Database,
        sql_validate::SqlValidator,
        template::TemplateEngine,
    },
    scheduler::job::JobScheduleMetadata,
};

#[derive(Debug, Clone)]
struct ProgressTracker {
    total_rows: u64,
    elapsed_time: f64,
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self {
            total_rows: 0,
            elapsed_time: 0.0,
        }
    }
}

fn humanize_time(seconds: f64) -> String {
    if seconds < 1.0 {
        return format!("{:.0}ms", seconds * 1000.0);
    }

    let total_seconds = seconds as u64;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let secs = total_seconds % 60;
    let millis = ((seconds % 1.0) * 1000.0) as u64;
    let mut parts = Vec::new();

    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if minutes > 0 {
        parts.push(format!("{}m", minutes));
    }
    if secs > 0 || parts.is_empty() {
        if millis > 0 && parts.is_empty() {
            parts.push(format!("{}.{:03}s", secs, millis));
        } else {
            parts.push(format!("{}s", secs));
        }
    }
    parts.join(" ")
}

pub async fn process_cleanup_task(
    metadata: &JobScheduleMetadata,
    config: &Config,
    task: &CleanupTask,
) -> Result<(), anyhow::Error> {
    let progress_tracker = Arc::new(Mutex::new(ProgressTracker::default()));
    let progress_tracker_clone = Arc::clone(&progress_tracker);
    let timeout_duration = Duration::from_secs_f64(task.task_timeout_seconds);

    match timeout(
        timeout_duration,
        execute_cleanup_task(metadata, config, task, progress_tracker_clone),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            // Timeout flow
            let progress = {
                let tracker = progress_tracker.lock().unwrap();
                tracker.clone()
            };
            let error_message = format!(
                "Task '{}' exceeded timeout limit of {} seconds and was stopped",
                task.name, task.task_timeout_seconds
            );
            warn!("{}", error_message);

            if config.slack_config.enabled {
                let slack_client = SlackClient::new(config.slack_config.bot_token.clone());
                let timeout_report = create_timeout_report(&CleanupMetadata {
                    config,
                    task,
                    total_rows: progress.total_rows,
                    elapsed_time: progress.elapsed_time,
                    schema_name: task.parameters.get("schema_name"),
                    table_name: task.parameters.get("table_name"),
                });

                let send_result = timeout_report
                    .send_to_channel(&slack_client, config.slack_config.channel_id.clone())
                    .await;

                if let Err(e) = send_result {
                    warn!("Failed to send timeout report to Slack: {}", e);
                } else {
                    info!("Timeout report sent to Slack");
                }
            }

            Err(anyhow::anyhow!("{}", error_message))
        }
    }
}

async fn execute_cleanup_task(
    metadata: &JobScheduleMetadata,
    config: &Config,
    task: &CleanupTask,
    progress_tracker: Arc<Mutex<ProgressTracker>>,
) -> Result<(), anyhow::Error> {
    // Initialize components
    let db = match Database::new(&config.database_config).await {
        Ok(db) => db,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Failed to initialize database connection: {}",
                e
            ))
        }
    };
    let template_engine = TemplateEngine::new();

    // Calculate intervals
    let data_interval_end = metadata
        .data_interval_end
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    info!("data_interval_end: {}", data_interval_end);

    let slack_client = if config.slack_config.enabled {
        Some(SlackClient::new(config.slack_config.bot_token.clone()))
    } else {
        None
    };

    if !task.enabled {
        info!("Skipping disabled task: {}", task.name);
        return Ok(());
    }

    info!("Processing cleanup task: {}", task.name);

    // Render SQL template
    let mut template_parameters = task.parameters.clone();
    template_parameters.insert("batch_size".to_string(), task.batch_size.to_string());
    let sql = template_engine.render(
        &task.template_query,
        &template_parameters,
        &data_interval_end,
    )?;

    // Validate SQL query
    if config.safe_mode.enabled {
        let validator = SqlValidator::new(config);
        let validate_result = validator.validate_sql_query(&sql);
        if let Err(e) = validate_result {
            let error_report = create_error_report(
                &CleanupMetadata {
                    config,
                    task,
                    total_rows: 0,
                    elapsed_time: 0.0,
                    schema_name: task.parameters.get("schema_name"),
                    table_name: task.parameters.get("table_name"),
                },
                &format!(
                "SQL validation failed for task: {}, error: {}. If unexpected, please consider switching safe_mode.enabled to false otherwise the Kiyoshi might be lacking support in ensuring that the query is safe to run",
                task.name, e
            ));
            if let Some(slack_client) = &slack_client {
                let send_result = error_report
                    .send_to_channel(slack_client, config.slack_config.channel_id.clone())
                    .await;
                if let Err(e) = send_result {
                    warn!("Failed to send error report to Slack: {}", e);
                } else {
                    info!("Error report sent to Slack");
                }
            }
            return Err(anyhow::anyhow!(
                "SQL validation failed for task: {}, error: {}",
                task.name,
                e
            ));
        }
    }

    info!("Executing cleanup query for task: {}", task.name);

    // Execute with retries
    let mut attempt = 0;
    let mut success = false;
    let mut total_rows: u64 = 0;
    let mut total_time_elapsed: f64 = 0.0;

    'outer: while attempt < task.retry_attempts {
        loop {
            info!("Executing sql query: \n{}", sql);
            match db.execute_query(&sql).await {
                Ok((affected_rows, elapsed_in_secs)) => {
                    if affected_rows == 0 {
                        info!(
                            "No more rows to clean up. Total rows cleaned: {} for task: {} in {}",
                            total_rows,
                            task.name,
                            humanize_time(elapsed_in_secs)
                        );
                        success = true;
                        let report = create_cleanup_report(CleanupMetadata {
                            config,
                            task,
                            total_rows,
                            elapsed_time: total_time_elapsed,
                            schema_name: task
                                .parameters
                                .get("schema_name")
                                .or(Some(&config.database_config.database)),
                            table_name: task.parameters.get("table_name"),
                        });
                        if let Some(slack_client) = &slack_client {
                            let send_result = report
                                .send_to_channel(
                                    slack_client,
                                    config.slack_config.channel_id.clone(),
                                )
                                .await;
                            if let Err(e) = send_result {
                                warn!("Failed to send cleanup report to Slack: {}", e);
                            } else {
                                info!("Cleanup report sent to Slack");
                            }
                        }
                        break 'outer;
                    }
                    total_time_elapsed += elapsed_in_secs;
                    total_rows += affected_rows;

                    {
                        let mut tracker = progress_tracker.lock().unwrap();
                        tracker.total_rows = total_rows;
                        tracker.elapsed_time = total_time_elapsed;
                    }

                    info!(
                        "Successfully cleaned up {} rows (total: {}) for task: {} in {}",
                        affected_rows,
                        total_rows,
                        task.name,
                        humanize_time(elapsed_in_secs)
                    );
                    tokio::time::sleep(Duration::from_secs_f64(task.query_interval_seconds)).await;
                }
                Err(e) => {
                    attempt += 1;
                    warn!(
                        "Attempt {}/{} failed for task {}: {}",
                        attempt, task.retry_attempts, task.name, e
                    );
                    if attempt < task.retry_attempts {
                        tokio::time::sleep(Duration::from_secs(task.retry_delay_seconds.into()))
                            .await;
                    }
                    if attempt == task.retry_attempts {
                        let error_report = create_error_report(
                            &CleanupMetadata {
                                config,
                                task,
                                total_rows,
                                elapsed_time: total_time_elapsed,
                                schema_name: task
                                    .parameters
                                    .get("schema_name")
                                    .or(Some(&config.database_config.database)),
                                table_name: task.parameters.get("table_name"),
                            },
                            &format!("All attempts failed for task: {}, error: {}", task.name, e),
                        );
                        if let Some(slack_client) = &slack_client {
                            let send_result = error_report
                                .send_to_channel(
                                    slack_client,
                                    config.slack_config.channel_id.clone(),
                                )
                                .await;
                            if let Err(e) = send_result {
                                warn!("Failed to send error report to Slack: {}", e);
                            } else {
                                info!("Error report sent to Slack");
                            }
                        }
                    }
                    break; // Break inner loop to retry with attempt counter
                }
            }
        }
    }

    if !success {
        warn!("All attempts failed for task: {}", task.name);
        return Err(anyhow::anyhow!(
            "All attempts failed for task: {}",
            task.name
        ));
    }

    info!("Cleanup process completed");
    Ok(())
}

struct CleanupMetadata<'a> {
    config: &'a Config,
    task: &'a CleanupTask,
    total_rows: u64,
    elapsed_time: f64,
    schema_name: Option<&'a String>,
    table_name: Option<&'a String>,
}

fn create_cleanup_report(metadata: CleanupMetadata) -> CreateMessage {
    let schema_table = match (metadata.schema_name, metadata.table_name) {
        (Some(schema), Some(table)) => format!("{}.{}", schema, table),
        (None, Some(table)) => table.clone(),
        (Some(schema), None) => schema.clone(),
        (None, None) => "Unknown Target".to_string(),
    };

    CreateMessage::Blocks(serde_json::json!([
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "🧹 *Cleanup Task Completed*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!("*Host:* `{}`\n*Task:* `{}`\n*Target:* `{}`", metadata.config.database_config.host, metadata.task.name, schema_table)
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": format!("*Total Rows Cleaned:*\n{}", metadata.total_rows)
                },
                {
                    "type": "mrkdwn",
                    "text": format!("*Total Time Elapsed:*\n{}", humanize_time(metadata.elapsed_time))
                }
            ]
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": format!("🕒 Completed: {} | 🫧 Kiyoshi Cleanup Service",
                        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                    )
                }
            ]
        }
    ]))
}

fn create_timeout_report(metadata: &CleanupMetadata) -> CreateMessage {
    let schema_table = match (metadata.schema_name, metadata.table_name) {
        (Some(schema), Some(table)) => format!("{}.{}", schema, table),
        (None, Some(table)) => table.clone(),
        (Some(schema), None) => schema.clone(),
        (None, None) => "Unknown Target".to_string(),
    };

    let mut blocks = vec![
        serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⏰ *Cleanup Task Timed Out*"
            }
        }),
        serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!("*Host:* `{}`\n*Task:* `{}`\n*Target:* `{}`", metadata.config.database_config.host, metadata.task.name, schema_table)
            }
        }),
        serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!("Task timed out after {} seconds\n", metadata.task.task_timeout_seconds)
            }
        }),
    ];

    if metadata.elapsed_time > 0.0 {
        blocks.push(serde_json::json!({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": format!("*Rows Cleaned:*\n{}", metadata.total_rows)
                },
                {
                    "type": "mrkdwn",
                    "text": format!("*Time Elapsed:*\n{}", humanize_time(metadata.elapsed_time))
                }
            ]
        }));
    }

    blocks.extend(vec![
        serde_json::json!({
            "type": "divider"
        }),
        serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⚠️ *Action Required:* Please check the logs and investigate the issue."
            }
        }),
        serde_json::json!({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": format!("🚨 Timed Out: {} | 🫧 Kiyoshi Cleanup Service",
                        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                    )
                }
            ]
        }),
    ]);

    CreateMessage::Blocks(serde_json::json!(blocks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_humanize_time() {
        assert_eq!(humanize_time(0.1), "100ms");
        assert_eq!(humanize_time(0.5), "500ms");
        assert_eq!(humanize_time(0.999), "999ms");
        assert_eq!(humanize_time(1.0), "1s");
        assert_eq!(humanize_time(30.0), "30s");
        assert_eq!(humanize_time(1.12345), "1.123s");
        assert_eq!(humanize_time(60.0), "1m");
        assert_eq!(humanize_time(90.0), "1m 30s");
        assert_eq!(humanize_time(3599.0), "59m 59s");
        assert_eq!(humanize_time(3600.0), "1h");
        assert_eq!(humanize_time(3661.0), "1h 1m 1s");
        assert_eq!(humanize_time(7260.0), "2h 1m");
        assert_eq!(humanize_time(93784.0), "26h 3m 4s");
    }
}

fn create_error_report(metadata: &CleanupMetadata, error: &str) -> CreateMessage {
    let schema_table = match (metadata.schema_name, metadata.table_name) {
        (Some(schema), Some(table)) => format!("{}.{}", schema, table),
        (None, Some(table)) => table.clone(),
        (Some(schema), None) => schema.clone(),
        (None, None) => "Unknown Target".to_string(),
    };
    CreateMessage::Blocks(serde_json::json!([
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "❌ *Cleanup Task Failed*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!("*Host:* `{}`\n*Task:* `{}`\n*Target:* `{}`", metadata.config.database_config.host, metadata.task.name, schema_table)
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!("*Error Details:*\n```\n{}\n```", error)
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⚠️ *Action Required:* Please check the logs and investigate the issue."
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": format!("🚨 Failed: {} | 🫧 Kiyoshi Cleanup Service",
                        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                    )
                }
            ]
        }
    ]))
}
