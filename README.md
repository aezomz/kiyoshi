# 🫧 Kiyoshi 🧹
**The release is still in Early Stage & ALPHA Version**

**Warning**: This tool can perform destructive database operations. Always test your cleanup tasks in a development environment before deploying to production. **Use safe mode and proper backup procedures.**

## Overview
Kiyoshi focus on simplicity and convenience, a configurable database cleanup tool written in Rust that schedules and executes SQL cleanup tasks based on YAML configuration. Perfect for deploying to kubernetes.

It supports templated SQL queries, batch processing, retry mechanisms, and Slack notifications.

## Features

- 🕒 **Cron-based Scheduling**: Schedule cleanup tasks using standard cron expressionsm up to seconds granularity
- 🗃️ **MySQL Support**: Currently only support for MySQL databases via SQLx
- 🛡️ **Safe Mode**: Protection against accidental data loss with retention policies
- 📢 **Slack Integration**: Optional notifications to Slack channels
- 🐳 **Deployment**: Ready-to-use Docker containers and perfect for kubernetes
- 🔧 **Environment Variable Support**: Flexible configuration with environment variable substitution, also support reading from a json file

## Installation

### Using Docker (Recommended)
Pull image from docker hub
https://hub.docker.com/r/aezomz/kiyoshi/tags

### Config file
Refer to [`config/example_config.yaml`](config/example_config.yaml) for the configuration file.

### Cleanup Tasks

Each cleanup task supports the following parameters:

- `name`: Unique identifier for the task
- `description`: Human-readable description
- `cron_schedule`: Cron expression supports both 5 fields (minutes granularity) and 6 fields (seconds granularity)
- `enabled`: Whether the task is active
- `template_query`: Jinja2-style SQL template
- `parameters`: Variables available in the template
- `batch_size`: Number of records to process per batch
- `retry_attempts`: Number of retry attempts on failure
- `retry_delay_seconds`: Delay between retries
- `query_interval_seconds`: Delay between batches
- `task_timeout_seconds`: Timeout for the task, default is 3600 seconds (1 hour). If the task takes longer than this, it will be stopped and a timeout report will be sent to Slack.

### Inherit Environment Variables in config file

```yaml
host: ${DB_HOST:-localhost}

# Without default (empty string if not set)
password: ${DB_PASSWORD}
```

### Safe Mode

Safe mode provides additional protection:

```yaml
safe_mode:
  enabled: true # only allow DELETE queries
  retention_days: 30  # Minimum retention period
```

## Command Line Options

```bash
kiyoshi [OPTIONS]

Options:
  -c, --config-file <CONFIG_FILE>  Path to the YAML configuration file [default: config.yaml]
  -e, --env-file <ENV_FILE>       Path to a JSON file containing environment variables
  -v, --verbose                   Enable verbose logging
  -h, --help                      Print help
  -V, --version                   Print version
```

### Project Structure

```
src/
├── main.rs              # Application entry point
├── cleaner/             # Core cleanup functionality
│   ├── config.rs        # Configuration parsing
│   ├── task.rs          # Task execution logic
│   ├── template.rs      # SQL template processing
│   ├── db.rs           # Database connections
│   └── sql_validate.rs  # SQL validation
└── scheduler/           # Cron scheduling
    ├── core.rs          # Scheduler implementation
    └── job.rs           # Job definitions
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For questions, bug reports, or feature requests, please open an issue on GitHub.

---