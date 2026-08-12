use std::time::Duration;

pub const DEFAULT_LEASE_SECONDS: i64 = 60;
pub const DEFAULT_TASK_TIMEOUT_SECONDS: u64 = 45;
pub const DEFAULT_MAX_DELIVERIES: i64 = 5;

pub fn lease_seconds() -> i64 {
    positive_i64_from_env("RUSTFLOW_LEASE_SECONDS", DEFAULT_LEASE_SECONDS)
}

pub fn task_timeout() -> Duration {
    Duration::from_secs(positive_u64_from_env(
        "RUSTFLOW_TASK_TIMEOUT_SECONDS",
        DEFAULT_TASK_TIMEOUT_SECONDS,
    ))
}

pub fn max_deliveries() -> i64 {
    positive_i64_from_env("RUSTFLOW_MAX_DELIVERIES", DEFAULT_MAX_DELIVERIES)
}

pub fn retry_backoff() -> Vec<Duration> {
    vec![
        Duration::from_secs(1),
        Duration::from_secs(5),
        Duration::from_secs(15),
        Duration::from_secs(30),
        Duration::from_secs(60),
    ]
}

fn positive_i64_from_env(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn positive_u64_from_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_has_one_delay_per_delivery() {
        assert_eq!(retry_backoff().len() as i64, max_deliveries());
    }
}
