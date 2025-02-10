mod cleaner;
mod scheduler;

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use cleaner::task;
use log::{info, warn};
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

    // Load environment variables from file
    if let Some(env_file_path) = cli.env_file {
        let env_content = std::fs::read_to_string(&env_file_path)
            .unwrap_or_else(|_| panic!("Failed to read env file: {}", env_file_path));

        let env_vars: std::collections::HashMap<String, String> =
            serde_json::from_str(&env_content)
                .unwrap_or_else(|_| panic!("Failed to parse env file as JSON: {}", env_file_path));

        for (key, value) in env_vars {
            std::env::set_var(key, value);
        }
    }

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

    // TODO: load from directory so we can run multiple config files
    // Load configuration from specified path
    let config = cleaner::config::Config::load_from_path(&cli.config_file)?;
    info!("Configuration loaded successfully from {}", cli.config_file);

    let mut scheduler = Scheduler::default();
    let configs = vec![config];
    for config in configs {
        let config_clone = config.clone();
        scheduler.add(
            Job::new("cleanup_task", &config.cron_schedule, move |metadata| {
                let config = config_clone.clone();
                Box::pin(async move {
                    if let Err(e) = task::process_cleanup_tasks(metadata, config).await {
                        warn!("Error running cleanup tasks: {}", e);
                    }
                })
            })
            .unwrap(),
        );
    }
    scheduler.add(
        Job::new("every 2", "*/2 * * * * *", move |_| {
            Box::pin(async {
                println!("{:?} - Every 2 seconds", Utc::now());
            })
        })
        .unwrap(),
    );

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
