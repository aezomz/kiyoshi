use std::{future::Future, pin::Pin, str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use cron::Schedule;
use log::info;

type JobFunction =
    (dyn FnMut(JobScheduleMetadata) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync);

pub struct Job {
    name: String,
    schedule: Schedule,
    function: Box<JobFunction>,
    last_run: Option<DateTime<Utc>>,
    schedule_metadata: JobScheduleMetadata,
}

pub struct JobScheduleMetadata {
    pub data_interval_end: DateTime<Utc>,
}

impl JobScheduleMetadata {
    pub fn new(data_interval_end: DateTime<Utc>) -> Self {
        Self { data_interval_end }
    }

    pub fn update(&mut self, data_interval_end: DateTime<Utc>) {
        self.data_interval_end = data_interval_end;
    }
}

impl Job {
    pub fn new<T, S>(name: S, schedule: &str, function: T) -> Result<Self, cron::error::Error>
    where
        S: Into<String>,
        T: FnMut(JobScheduleMetadata) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let schedule = Schedule::from_str(schedule)?;
        let now = Utc::now();
        let upcoming = Self::get_next_schedule(&schedule, now);

        Ok(Self {
            name: name.into(),
            schedule,
            function: Box::new(function),
            last_run: None,
            schedule_metadata: JobScheduleMetadata::new(upcoming),
        })
    }

    pub fn get_next_schedule(schedule: &Schedule, now: DateTime<Utc>) -> DateTime<Utc> {
        schedule.after(&now).next().unwrap_or(now)
    }

    #[must_use]
    pub fn until(&self) -> Option<Duration> {
        let reference_time = if self.last_run.is_some() {
            self.schedule_metadata.data_interval_end
        } else {
            Utc::now()
        };

        if let Some(upcoming) = self.schedule.after(&reference_time).next() {
            return if let Ok(duration_until) = upcoming.signed_duration_since(Utc::now()).to_std() {
                Some(duration_until)
            } else {
                Some(Duration::from_secs(0))
            };
        }
        None
    }

    pub async fn run(&mut self) {
        let now = Utc::now();
        // Use current scheduled time for this execution
        let current_scheduled_time = self.schedule_metadata.data_interval_end;

        // Calculate next scheduled time for future runs
        let next = Self::get_next_schedule(&self.schedule, current_scheduled_time);

        let metadata = JobScheduleMetadata {
            data_interval_end: current_scheduled_time,
        };

        info!("{:?} firing: `{}`", now, self.name);
        self.last_run = Some(now);

        // Update the stored metadata for next run
        self.schedule_metadata.update(next);

        let fut = (self.function)(metadata);
        tokio::spawn(async move {
            fut.await;
        });
        info!("completed: `{}`, next run will be at {}", self.name, next);
    }
    #[allow(dead_code)]
    pub fn get_schedule_metadata(&self) -> &JobScheduleMetadata {
        &self.schedule_metadata
    }
}
