use std::{future::Future, pin::Pin, str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use cron::Schedule;

pub struct Job {
    name: String,
    schedule: Schedule,
    function: Box<(dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync)>,

    last_run: Option<DateTime<Utc>>,
}

impl Job {
    pub fn new<T, S>(name: S, schedule: &str, function: T) -> Result<Self, cron::error::Error>
    where
        S: Into<String>,
        T: FnMut() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        Ok(Self {
            name: name.into(),
            schedule: Schedule::from_str(schedule)?,
            function: Box::new(function),
            last_run: None,
        })
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
        println!("{:?} firing: `{}`", Utc::now(), self.name);
        self.last_run = Some(Utc::now());
        let fut = (self.function)();
        tokio::spawn(async move {
            fut.await;
        });
    }
}
