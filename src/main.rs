mod cleaner;
mod scheduler;

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use cleaner::task;
use log::{error, info, warn};
use scheduler::{core::Scheduler, job::Job};
use tokio::signal;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the YAML configuration file
    #[arg(short, long, default_value = "config.yaml")]
    config_file: String,

    /// Path to a JSON file containing environment variables
    #[arg(short, long)]
    env_file: Option<String>,

    /// Optional: Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Initialize logging with optional verbose mode
    let log_level = if cli.verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    env_logger::Builder::new()
        .filter_level(log_level)
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "{} {} [{}:{}] {}",
                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .init();

    if let Some(env_file_path) = cli.env_file {
        if let Err(e) = cleaner::config::load_env_from_file(&env_file_path) {
            error!("Failed to load environment file: {}", e);
            return Err(e);
        }
    }

    // TODO: load from directory so we can run multiple config files
    // Load configuration from specified path
    let config = cleaner::config::FullConfig::load_from_path(&cli.config_file)?;
    info!("Configuration loaded successfully from {}", cli.config_file);

    let mut scheduler = Scheduler::default();
    let full_configs = vec![config];
    for full_config in full_configs {
        for task in full_config.cleanup_tasks {
            let config_clone = full_config.config.clone();
            let task_clone = task.clone();
            scheduler.add(
                Job::new("cleanup_task", &task.cron_schedule, move |metadata| {
                    let config = config_clone.clone();
                    let task = task_clone.clone();
                    Box::pin(async move {
                        if let Err(e) = task::process_cleanup_tasks(&metadata, &config, &task).await
                        {
                            warn!("Error running cleanup tasks: {}", e);
                        }
                    })
                })
                .unwrap(),
            );
        }
    }
    // scheduler.add(
    //     Job::new("every 2", "*/2 * * * * *", move |_| {
    //         Box::pin(async {
    //             println!("{:?} - Every 2 seconds", Utc::now());
    //         })
    //     })
    //     .unwrap(),
    // );

    // Start the scheduler in the background
    let scheduler_handle = tokio::spawn(async move {
        scheduler.start().await;
    });

    // Wait for shutdown signal (Ctrl+C or SIGTERM)
    info!("Server running. Press Ctrl+C or send SIGTERM to stop");
    shutdown_signal().await;
    info!("Shutdown signal received, stopping gracefully...");

    // Cancel the scheduler task
    scheduler_handle.abort();
    info!("Scheduler stopped");
    info!("Shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
