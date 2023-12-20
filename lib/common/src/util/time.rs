use std::{
    fmt::Write,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Pretty-print the elapsed time (used in progress bars)
pub fn elapsed_subsec(state: &indicatif::ProgressState, writer: &mut dyn Write) {
    let seconds = state.elapsed().as_secs();
    let sub_seconds = (state.elapsed().as_millis() % 1000) / 100;
    let _ = writer.write_str(&format!("{}.{}s", seconds, sub_seconds));
}

/// Pretty-print the elapsed time (used in logs and diagnostics)
pub fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    let micros = nanos / 1_000;
    let millis = duration.as_millis();
    let seconds = duration.as_secs();
    let minutes = seconds / 60;
    let hours = seconds / 3_600;
    let days = seconds / 86_400;

    match days {
        0 => match hours {
            0 => match minutes {
                0 => match seconds {
                    0 => match millis {
                        0 => format!(
                            "{:.3}μs",
                            micros as f64 + (nanos % 1_000) as f64 / 1_000_000.0
                        ),
                        _ => format!("{:.3}s", seconds as f64 + millis as f64 / 1_000.0),
                    },
                    _ => format!("{:.3}m", minutes as f64 + seconds as f64 / 60.0),
                },
                _ => format!("{:.3}h", hours as f64 + minutes as f64 / 60.0),
            },
            _ => format!("{:.3}d", days as f64 + hours as f64 / 24.0),
        },
        _ => format!("{}d", days),
    }
}

/// Returns the current time in nanoseconds since the UNIX epoch.
///
/// # Panics
///
/// Panics if the current time is before the UNIX epoch (i.e. if the system clock is set
/// incorrectly).
pub fn now_as_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos() as u64
}

pub fn elapsed_duration_since(start_time: u64) -> Duration {
    Duration::from_nanos(now_as_u64().saturating_sub(start_time))
}
