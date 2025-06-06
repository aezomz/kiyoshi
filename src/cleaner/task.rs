use anyhow::Result;
use log::{info, warn};
use serde_json;
use slack_api_client::{CreateMessage, SlackClient};
use tokio::time::Duration;

use crate::{
    cleaner::{
        config::{CleanupTask, Config},
        db::Database,
        sql_validate::SqlValidator,
        template::TemplateEngine,
    },
    scheduler::job::JobScheduleMetadata,
};

pub async fn process_cleanup_task(
    metadata: &JobScheduleMetadata,
    config: &Config,
    task: &CleanupTask,
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
                                "No more rows to clean up. Total rows cleaned: {} for task: {} in {:.2}s",
                                total_rows, task.name, elapsed_in_secs
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
                    info!(
                        "Successfully cleaned up {} rows (total: {}) for task: {} in {:.2}s",
                        affected_rows, total_rows, task.name, elapsed_in_secs
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
                    "text": format!("*Total Time Elapsed:*\n{:.2}s", metadata.elapsed_time)
                }
            ]
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": format!("📅 Completed: {} | 🫧 Kiyoshi Cleanup Service",
                        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                    )
                }
            ]
        }
    ]))
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
