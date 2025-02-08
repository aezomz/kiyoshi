use anyhow::Result;
use chrono::Utc;
use log::{info, warn};
use tokio::time::Duration;

use crate::cleaner::{config::Config, db::Database, template::TemplateEngine};

pub async fn process_cleanup_tasks(config: Config) -> Result<(), anyhow::Error> {
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
    let end_time = Utc::now();
    let prev_run = template_engine.get_previous_schedule(&config.cron_schedule, end_time)?;
    let interval_end = end_time.format("%Y-%m-%d %H:%M:%S").to_string();
    let interval_start = prev_run.format("%Y-%m-%d %H:%M:%S").to_string();

    // Process each cleanup task
    for task in config.cleanup_tasks {
        if !task.enabled {
            info!("Skipping disabled task: {}", task.name);
            continue;
        }

        info!("Processing cleanup task: {}", task.name);

        // Render SQL template
        let sql = template_engine.render(
            &task.template_query,
            &task.parameters,
            &interval_start,
            &interval_end,
        )?;

        info!("Executing cleanup query for task: {}", task.name);

        // Execute with retries
        let mut attempt = 0;
        let mut success = false;
        let mut total_rows = 0;

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
                            break 'outer;
                        }
                        total_rows += affected_rows;
                        info!(
                            "Successfully cleaned up {} rowss (total: {}) for task: {} in {:.2}s",
                            affected_rows, total_rows, task.name, elapsed_in_secs
                        );
                        tokio::time::sleep(Duration::from_secs(task.query_interval_seconds.into()))
                            .await;
                    }
                    Err(e) => {
                        attempt += 1;
                        warn!(
                            "Attempt {}/{} failed for task {}: {}",
                            attempt, task.retry_attempts, task.name, e
                        );
                        if attempt < task.retry_attempts {
                            tokio::time::sleep(Duration::from_secs(
                                task.retry_delay_seconds.into(),
                            ))
                            .await;
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
    }

    info!("Cleanup process completed");
    Ok(())
}
