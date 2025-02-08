use anyhow::Result;
use chrono::{DateTime, Utc};
use cron::Schedule;
use minijinja::Environment;
use std::{collections::HashMap, str::FromStr};

pub struct TemplateEngine {
    env: Environment<'static>,
}

impl TemplateEngine {
    pub fn new() -> Self {
        Self {
            env: Environment::new(),
        }
    }

    pub fn render(
        &self,
        template: &str,
        params: &HashMap<String, String>,
        data_interval_start: &str,
        data_interval_end: &str,
    ) -> Result<String> {
        let mut context = params.clone();
        context.insert(
            "data_interval_start".to_string(),
            data_interval_start.to_string(),
        );
        context.insert(
            "data_interval_end".to_string(),
            data_interval_end.to_string(),
        );

        let tmpl = self.env.template_from_str(template)?;
        let rendered = tmpl.render(context)?;
        Ok(rendered)
    }
    pub fn get_previous_schedule(
        &self,
        schedule: &str,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        // Parse cron expression
        let schedule = Schedule::from_str(schedule)?;
        let prev_run = schedule
            .after(&now)
            .take(1)
            .next()
            .ok_or_else(|| anyhow::anyhow!("Could not calculate previous run time"))?;

        println!("prev_run: {}, now: {}", prev_run, now);
        Ok(prev_run)
    }
}

impl Default for TemplateEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_template_render() -> Result<()> {
        let engine = TemplateEngine::new();
        let mut params = HashMap::new();
        params.insert("name".to_string(), "test".to_string());
        params.insert("value".to_string(), "123".to_string());

        let template = "Hello {{ name }}! Value is {{ value }}. Start: {{ data_interval_start }}, End: {{ data_interval_end }}";
        let result = engine.render(template, &params, "2024-01-01", "2024-01-02")?;

        assert_eq!(
            result,
            "Hello test! Value is 123. Start: 2024-01-01, End: 2024-01-02"
        );
        Ok(())
    }

    #[test]
    fn test_template_render_empty_params() -> Result<()> {
        let engine = TemplateEngine::new();
        let params = HashMap::new();
        let template = "Start: {{ data_interval_start }}, End: {{ data_interval_end }}";
        let result = engine.render(template, &params, "2024-01-01", "2024-01-02")?;

        assert_eq!(result, "Start: 2024-01-01, End: 2024-01-02");
        Ok(())
    }

    #[test]
    fn test_template_render_invalid_template() {
        let engine = TemplateEngine::new();
        let params = HashMap::new();
        let template = "Hello {{ invalid syntax";
        let result = engine.render(template, &params, "2024-01-01", "2024-01-02");

        assert!(result.is_err());
    }

    #[test]
    fn test_get_previous_schedule() -> Result<()> {
        let engine = TemplateEngine::new();

        // Test with a fixed time for deterministic results
        let fixed_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let schedule = "0 0 0 * * *"; // Daily at midnight

        let result = engine.get_previous_schedule(schedule, fixed_time)?;
        assert_eq!(
            result.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2024-01-02 00:00:00"
        );
        assert!(result > fixed_time);

        Ok(())
    }

    #[test]
    fn test_get_previous_schedule_invalid_cron() {
        let engine = TemplateEngine::new();
        let invalid_schedule = "invalid cron";
        let result = engine.get_previous_schedule(invalid_schedule, Utc::now());

        assert!(result.is_err());
    }
}
