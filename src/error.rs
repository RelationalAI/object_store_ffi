use std::time::Duration;
use backoff::backoff::Backoff;
use once_cell::sync::Lazy;
use crate::{Config, ConfigMeta, clients};

// These regexes are used to extract error info from some object_store private errors.
// We construct the regexes lazily and reuse them due to the runtime compilation cost.
static CLIENT_ERR_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"Client \{ status: (?<status>\d+?),").unwrap());
static REQWEST_ERR_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"Reqwest \{ retries: (?<retries>\d+?), max_retries: (?<max_retries>\d+?),").unwrap());


#[derive(Debug)]
pub(crate) struct ErrorInfo {
    pub(crate) retries: Option<usize>,
    pub(crate) reason: ErrorReason
}

#[derive(Debug)]
pub(crate) enum ErrorReason {
    Unknown,
    Code(u16),
    Io,
    Timeout
}

pub(crate) fn backoff_duration_for_retry(retries: usize, meta: &ConfigMeta) -> Duration {
    // We try to use the same settings as the object_store backoff but the implementation is
    // different so this is best effort.
    let mut backoff = backoff::ExponentialBackoff {
        initial_interval: meta.retry_config.backoff.init_backoff,
        max_interval: meta.retry_config.backoff.max_backoff,
        ..Default::default()
    };

    for _retry in 0..retries {
        let _ = backoff.next_backoff();
    }

    backoff.next_backoff().unwrap_or(meta.retry_config.backoff.max_backoff)
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

pub(crate) async fn should_retry(retries: usize, error: &anyhow::Error, elapsed: Duration, config: &Config) -> Option<Duration> {
    let Some((_, meta)) = clients().get(&config.get_hash()).await else { return None };
    if should_retry_logic(retries, error, elapsed, &meta) {
        Some(backoff_duration_for_retry(retries, &meta))
    } else {
        None
    }
}

pub(crate) fn should_retry_logic(retries: usize, error: &anyhow::Error, elapsed: Duration, meta: &ConfigMeta) -> bool {
    let info = extract_error_info(error);
    let max_retries = meta.retry_config.max_retries;
    // Don't retry errors that were fully retried already
    let store_retries = match info.retries {
        Some(store_retries) => {
            if store_retries + retries >= max_retries {
                return false
            }
            store_retries
        },
        None => 0
    };

    let all_retries = retries + store_retries;

    match info.reason {
        ErrorReason::Timeout => {
            // Retry timeouts up to retry_timeout or max_retries
            all_retries < max_retries && elapsed < meta.retry_config.retry_timeout
        }
        ErrorReason::Code(_code) => {
            // TODO manage custom status_code retries
            false
        }
        ErrorReason::Io => {
            // Retry io errors up to retry_timeout or max_retries
            all_retries < max_retries && elapsed < meta.retry_config.retry_timeout
        }
        ErrorReason::Unknown => {
            false
        }
    }
}
