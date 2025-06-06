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

#[derive(Clone, Copy)]
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
        if let Some(upcoming) = self
            .schedule
            .after(&self.last_run.unwrap_or_else(Utc::now))
            .next()
        {
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
        info!("Task `{}` firing at {}", self.name, now);
        self.last_run = Some(now);

        let fut = (self.function)(self.schedule_metadata);
        tokio::spawn(async move {
            fut.await;
        });
        let next =
            Self::get_next_schedule(&self.schedule, self.schedule_metadata.data_interval_end);
        self.schedule_metadata.update(next);
        info!("Task `{}`, next run will be at {}", self.name, next);
    }
    #[allow(dead_code)]
    pub fn get_schedule_metadata(&self) -> &JobScheduleMetadata {
        &self.schedule_metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::str::FromStr;

    #[derive(Debug)]
    struct GetNextScheduleTestCase {
        name: &'static str,
        cron_expression: &'static str,
        now: DateTime<Utc>,
        expected: DateTime<Utc>,
    }

    #[test]
    fn test_get_next_schedule_parameterized() {
        let test_cases = vec![
            GetNextScheduleTestCase {
                name: "daily at midnight",
                cron_expression: "0 0 0 * * *",
                now: Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap(),
                expected: Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap(),
            },
            GetNextScheduleTestCase {
                name: "hourly at minute 0",
                cron_expression: "0 0 * * * *",
                now: Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 0).unwrap(),
                expected: Utc.with_ymd_and_hms(2024, 1, 1, 13, 0, 0).unwrap(),
            },
            GetNextScheduleTestCase {
                name: "every minute",
                cron_expression: "0 * * * * *",
                now: Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 30).unwrap(),
                expected: Utc.with_ymd_and_hms(2024, 1, 1, 12, 31, 0).unwrap(),
            },
            GetNextScheduleTestCase {
                name: "every minute (exact round minute now)",
                cron_expression: "0 * * * * *",
                now: Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 0).unwrap(),
                expected: Utc.with_ymd_and_hms(2024, 1, 1, 12, 31, 0).unwrap(),
            },
        ];

        for test_case in test_cases {
            let schedule = Schedule::from_str(test_case.cron_expression).unwrap_or_else(|_| {
                panic!(
                    "Failed to parse cron expression for test case: {}",
                    test_case.name
                )
            });

            let next = Job::get_next_schedule(&schedule, test_case.now);

            assert_eq!(
                next, test_case.expected,
                "Schedule mismatch for test case: '{}' - expected {}, got {}",
                test_case.name, test_case.expected, next
            );
        }
    }
}
