mod cleaner;
mod scheduler;

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use cleaner::task;
use log::{info, warn};
use scheduler::{core::Scheduler, job::Job};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the YAML configuration file
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

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

    // TODO: load from directory so we can run multiple config files
    // Load configuration from specified path
    let config = cleaner::config::Config::load_from_path(&cli.config)?;
    info!("Configuration loaded successfully from {}", cli.config);

    let mut scheduler = Scheduler::default();
    let configs = vec![config];
    for config in configs {
        let config_clone = config.clone();
        scheduler.add(
            Job::new("cleanup_task", &config.cron_schedule, move || {
                let config = config_clone.clone();
                Box::pin(async move {
                    if let Err(e) = task::process_cleanup_tasks(config).await {
                        warn!("Error running cleanup tasks: {}", e);
                    }
                })
            })
            .unwrap(),
        );
    }
    scheduler.add(
        Job::new("every 2", "*/2 * * * * *", || {
            Box::pin(async {
                println!("{:?} - Every 2 seconds", Utc::now());
            })
        })
        .unwrap(),
    );
    scheduler.start().await;
    Ok(())
}
