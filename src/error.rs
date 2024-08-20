use std::time::Duration;
use backoff::backoff::Backoff;
use object_store::RetryConfig;
use once_cell::sync::Lazy;
use std::error::Error as StdError;
use anyhow::anyhow;

// These regexes are used to extract error info from some object_store private errors.
// We construct the regexes lazily and reuse them due to the runtime compilation cost.
static CLIENT_ERR_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"Client \{ status: (?<status>\d+?),").unwrap());
static REQWEST_ERR_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"Reqwest \{ retries: (?<retries>\d+?), max_retries: (?<max_retries>\d+?),").unwrap());


#[derive(Debug, Clone)]
pub(crate) struct ErrorInfo {
    pub(crate) retries: Option<usize>,
    pub(crate) reason: ErrorReason
}

#[derive(Debug, Clone)]
pub(crate) enum ErrorReason {
    Unknown,
    Code(u16),
    Io,
    Timeout
}

pub(crate) fn format_err(error: &anyhow::Error) -> String {
    use std::fmt::Write;
    let mut error_string = format!("{}\n\nCaused by:\n", error);
    error.chain()
        .skip(1)
        .enumerate()
        .for_each(|(idx, cause)| write!(error_string, "    {}: {}\n", idx, cause).unwrap());
    error_string
}

pub(crate) fn extract_error_info(error: &anyhow::Error) -> ErrorInfo {
    let mut retries = None;

    // Here we go through the chain of type erased errors that led to the current one
    // trying to downcast each to concrete types. We fallback to error string parsing only on
    // private errors (as we don't have the type) and mainly to extract helpfull information
    // on a best effort basis.
    for e in error.chain() {
        if let Some(e) = e.downcast_ref::<reqwest::Error>() {
            if let Some(code) = e.status() {
                return ErrorInfo {
                    retries,
                    reason: ErrorReason::Code(code.into())
                }
            }
            if e.is_timeout() {
                return ErrorInfo {
                    retries,
                    reason: ErrorReason::Timeout
                }
            }
            if e.is_body() || e.is_connect() || e.is_request() {
                return ErrorInfo {
                    retries,
                    reason: ErrorReason::Io
                }
            }
        }

        if let Some(e) = e.downcast_ref::<hyper::Error>() {
            if e.is_closed() || e.is_incomplete_message() || e.is_body_write_aborted() {
                return ErrorInfo {
                    retries,
                    reason: ErrorReason::Io
                }
            } else if e.is_timeout() {
                return ErrorInfo {
                    retries,
                    reason: ErrorReason::Timeout
                }
            }
        }

        if let Some(e) = e.downcast_ref::<std::io::Error>() {
            if e.kind() == std::io::ErrorKind::TimedOut {
                return ErrorInfo {
                    retries,
                    reason: ErrorReason::Timeout
                }
            } else if e.kind() == std::io::ErrorKind::Other && e.source().is_some() {
                // Continue to source error
                continue
            } else {
                return ErrorInfo {
                    retries,
                    reason: ErrorReason::Io
                }
            }
        }

        let error_debug = format!("{:?}", e);
        if error_debug.starts_with("Client {") {
            if let Some(caps) = CLIENT_ERR_REGEX.captures(&error_debug) {
                if let Ok(status) = caps["status"].parse() {
                    // If we find this error we try to extract the status code from its debug
                    // representation
                    return ErrorInfo {
                        retries,
                        reason: ErrorReason::Code(status)
                    }
                }
            }
        } else if error_debug.starts_with("Reqwest {") {
            if let Some(caps) = REQWEST_ERR_REGEX.captures(&error_debug) {
                // If we find this error we try to extract the retries from its debug
                // representation
                retries = caps["retries"].parse::<usize>().ok();
            }
        }
    }
    ErrorInfo { retries, reason: ErrorReason::Unknown }
}

#[derive(Debug)]
pub(crate) struct RetryState {
    pub(crate) start: std::time::Instant,
    pub(crate) attempts: Vec<ErrorInfo>,
    pub(crate) retry_config: RetryConfig
}

impl RetryState {
    pub(crate) fn new(retry_config: RetryConfig) -> RetryState {
        RetryState {
            start: std::time::Instant::now(),
            attempts: vec![],
            retry_config
        }
    }

    pub(crate) fn retries(&self) -> usize {
        let prev_retries = self.attempts.iter()
            .map(|a| a.retries.unwrap_or_default())
            .sum::<usize>();
        prev_retries + self.attempts.len().saturating_sub(1)
    }

    fn next_backoff(&self) -> Duration {
        // We try to use the same settings as the object_store backoff but the implementation is
        // different so this is best effort.
        let mut backoff = backoff::ExponentialBackoff {
            initial_interval: self.retry_config.backoff.init_backoff,
            max_interval: self.retry_config.backoff.max_backoff,
            ..Default::default()
        };


        for _retry in 0..self.retries() {
            let _ = backoff.next_backoff();
        }

        backoff.next_backoff().unwrap_or(self.retry_config.backoff.max_backoff)
    }

    fn log_attempt(&mut self, info: ErrorInfo) {
        self.attempts.push(info);
    }

    pub(crate) fn should_retry_logic(&self) -> bool {
        let max_retries = self.retry_config.max_retries;
        let retry_timeout = self.retry_config.retry_timeout;
        let elapsed = self.start.elapsed();
        let all_retries = self.retries();

        let Some(last_attempt) = self.attempts.iter().last() else { return true };

        match last_attempt.reason {
            ErrorReason::Timeout => {
                // Retry timeouts up to retry_timeout or max_retries
                all_retries < max_retries && elapsed < retry_timeout
            }
            ErrorReason::Code(_code) => {
                // TODO manage custom status_code retries
                false
            }
            ErrorReason::Io => {
                // Retry io errors up to retry_timeout or max_retries
                all_retries < max_retries && elapsed < retry_timeout
            }
            ErrorReason::Unknown => {
                false
            }
        }
    }

    pub(crate) fn retry_report(&self) -> String {
        use std::fmt::Write;
        let mut report = String::new();
        if !self.attempts.is_empty() {
            let attempts_to_display = self.attempts.len().min(10);
            write!(report, "Recent attempts ({} out of {}):\n", attempts_to_display, self.attempts.len()).unwrap();
            self.attempts
                .iter()
                .rev()
                .take(10)
                .rev()
                .for_each(|info| {
                    write!(
                        report,
                        "    reason: {:?} after {} retries\n",
                        info.reason,
                        info.retries.unwrap_or_default()
                    ).unwrap()
                });
        } else {
            write!(report, "There were no attempts\n").unwrap();
        }
        write!(report, "Total retries: {}\n", self.retries()).unwrap();
        write!(report, "Total Time: {:?}\n", self.start.elapsed()).unwrap();
        report
    }

    pub(crate) fn should_retry(&mut self, error: &anyhow::Error) -> anyhow::Result<(ErrorInfo, Duration)> {
        let info = extract_error_info(error);
        self.log_attempt(info.clone());
        let decision = if self.should_retry_logic() {
            Ok((info, self.next_backoff()))
        } else {
            let error_report = format_err(error);
            Err(anyhow!("{}\n{}", error_report, self.retry_report()))
        };
        decision
    }
}
