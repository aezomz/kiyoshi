use std::{ops::Add, time::Duration};

use super::job::Job;

#[derive(Default)]
pub struct Scheduler {
    jobs: Vec<Job>,
}

impl Scheduler {
    pub fn add(&mut self, job: Job) {
        self.jobs.push(job);
    }

    pub fn until(&mut self) -> Option<(Vec<&mut Job>, Duration)> {
        let mut next_jobs = Vec::new();
        let mut next_job_duration = None; // the time delta until the next run
        for job in &mut self.jobs {
            if let Some(duration) = job.until() {
                let duration_millis = duration.as_millis();
                if next_job_duration.is_none() || duration_millis < next_job_duration.unwrap() {
                    next_job_duration = Some(duration_millis);
                    next_jobs.clear();
                    next_jobs.push(job);
                } else if duration_millis == next_job_duration.unwrap() {
                    next_jobs.push(job);
                }
            }
        }
        if let Some(duration) = next_job_duration {
            return Some((next_jobs, Duration::from_millis(duration as u64)));
        }
        None
    }

    pub async fn start(&mut self) {
        loop {
            if let Some((jobs, duration)) = self.until() {
                // a hack to make sure we don't fire a job a few microseconds early
                tokio::time::sleep(duration.add(std::time::Duration::from_micros(700))).await;
                for job in jobs {
                    job.run().await;
                }
            } else {
                return;
            }
        }
    }
}
