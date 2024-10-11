use std::{sync::Arc, time::Duration};
use backoff::backoff::Backoff;
use object_store::RetryConfig;
use once_cell::sync::Lazy;
use std::error::Error as StdError;
use thiserror::Error;
use std::fmt;

// These regexes are used to extract error info from some object_store private errors.
// We construct the regexes lazily and reuse them due to the runtime compilation cost.
static CLIENT_ERR_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"Client \{ status: (?<status>\d+?),").unwrap());
static REQWEST_ERR_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"Reqwest \{ retries: (?<retries>\d+?), max_retries: (?<max_retries>\d+?),").unwrap());

type BoxedStdError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug)]
pub(crate) struct Metadata {
    start: std::time::Instant,
    attempts: Vec<ErrorInfo>
}

impl Metadata {
    pub(crate) fn retries(&self) -> usize {
        let prev_retries = self.attempts.iter()
            .map(|a| a.retries.unwrap_or_default())
            .sum::<usize>();
        prev_retries + self.attempts.len().saturating_sub(1)
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
}

#[derive(Debug)]
pub struct Error {
    kind: Kind,
    metadata: Option<Metadata>
}

impl<T: Into<Kind>> From<T> for Error {
    fn from(value: T) -> Self {
        Error { kind: value.into(), metadata: None }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.kind, formatter)?;
        if self.kind.source().is_some() {
            write!(formatter, "\n\nCaused by:\n")?;
            self.kind.chain()
                .skip(1)
                .enumerate()
                .map(|(idx, cause)| {
                    write!(formatter, "    {}: {}\n", idx, cause)
                })
                .collect::<Result<_, fmt::Error>>()?;
        }
        if let Some(metadata) = self.metadata.as_ref() {
            let report = metadata.retry_report();
            write!(formatter, "\n{}", report)?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.kind.source()
    }
}

impl Error {
    fn error_info(&self) -> ErrorInfo {
        let mut info = self.kind.error_info();
        if let Some(metadata) = self.metadata.as_ref() {
            let retries = metadata.retries();
            if retries > 0 {
                info.retries = Some(metadata.retries());
            }
        }
        info
    }
    #[allow(unused)]
    pub(crate) fn not_implemented(msg: impl Into<String>) -> Error { Kind::NotImplemented(msg.into()).into() }
    pub(crate) fn required_config(msg: impl Into<String>) -> Error { Kind::RequiredConfig(msg.into()).into() }
    pub(crate) fn invalid_config(msg: impl Into<String>) -> Error { Kind::InvalidConfig { msg: msg.into(), source: None }.into() }
    pub(crate) fn invalid_config_src(msg: impl Into<String>, source: impl Into<BoxedStdError>) -> Error {
        Kind::InvalidConfig { msg: msg.into(), source: Some(source.into()) }.into()
    }
    pub(crate) fn invalid_config_err<E>(msg: &'static str) -> impl Fn(E) -> Error
    where
        E: Into<Box<dyn StdError + Send + Sync + 'static>>
    {
        return |e: E| Kind::InvalidConfig { msg: msg.into(), source: Some(e.into()) }.into()
    }
    #[allow(unused)]
    pub(crate) fn deserialize_response(msg: impl Into<String>, error: serde_path_to_error::Error<serde_json::Error>) -> Error {
        Kind::DeserializeResponse { response: msg.into(), source: error }.into()
    }
    pub(crate) fn deserialize_response_err(msg: &'static str) -> impl Fn(serde_path_to_error::Error<serde_json::Error>) -> Error
    {
        return |e| Kind::DeserializeResponse { response: msg.into(), source: e }.into()
    }
    pub(crate) fn invalid_response(msg: impl Into<String>) -> Error { Kind::InvalidResponse(msg.into()).into() }
    pub(crate) fn error_response(msg: impl Into<String>) -> Error { Kind::ErrorResponse(msg.into()).into() }
}

pub(crate) trait ErrorExt<T> {
    fn to_err(self) -> Result<T, Error>;
}

impl<T> ErrorExt<T> for Result<T, anyhow::Error> {
    fn to_err(self) -> Result<T, Error> {
        self.map_err(|e| Kind::Other(e).into())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Kind {
    #[error("{0}")]
    Request(#[from] reqwest::Error),
    #[error("{0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("{0}")]
    ErrorResponse(String),
    #[error("{0}")]
    InvalidResponse(String),
    #[error("Failed to deserialize `{response}` response")]
    DeserializeResponse {
        response: String,
        #[source]
        source: serde_path_to_error::Error<serde_json::Error>
    },
    #[error("Storage referenced as `{0}` is not encrypted")]
    StorageNotEncrypted(String),
    #[error("{msg}")]
    InvalidConfig {
        msg: String,
        #[source]
        source: Option<BoxedStdError>
    },
    #[error("Missing required config `{0}`")]
    RequiredConfig(String),
    #[error("{0} is not implemented")]
    NotImplemented(String),
    #[error("failed to decode encryption material: {0}")]
    MaterialDecode(#[source] base64::DecodeError),
    #[error("failed to encrypt or decrypt encryption material: {0}")]
    MaterialCrypt(#[source] std::io::Error),
    #[error("failed to encrypt the object contents: {0}")]
    ContentEncrypt(#[source] std::io::Error),
    #[error("failed to decrypt the object contents: {0}")]
    ContentDecrypt(#[source] std::io::Error),
    #[error("Supplied buffer was too small")]
    BufferTooSmall,
    #[error("io error while streaming body: {0}")]
    BodyIo(#[source] std::io::Error),
    #[error("{0}")]
    TaskFailed(#[from] tokio::task::JoinError),
    #[allow(dead_code)]
    #[error("{context}")]
    Context {
        context: String,
        #[source]
        source: BoxedStdError
    },
    #[error("{0}")]
    Wrapped(#[from] Arc<Error>),
    #[error("{0}")]
    Other(#[source] anyhow::Error)
}

impl Kind {
//     pub(crate) fn retryable(&self) -> bool {
//         match self {
//             Kind::Request(_)
//             | Kind::InvalidResponse(_)
//             | Kind::DeserializeResponse { .. } => {
//                 true
//             },
//             Kind::StorageNotEncrypted(_)
//             | Kind::InvalidConfig { .. }
//             | Kind::RequiredConfig(_)
//             | Kind::ErrorResponse(_)
//             | Kind::NotImplemented(_) => {
//                 false
//             }
//             Kind::Wrapped(e) => e.kind.retryable(),
//             Kind::Context { source, .. } => {
//                 match source.downcast_ref::<Error>() {
//                     Some(e) => e.kind.retryable(),
//                     None => false
//                 }
//             }
//         }
//     }

    pub(crate) fn chain(&self) -> Chain {
        Chain(Some(self))
    }

    pub(crate) fn error_info(&self) -> ErrorInfo {
        if matches!(self, Kind::Request(_)) {
            self.chain()
                .for_each(|e| println!("----------\n{} {:?}", e, e));
        }
        let mut retries = None;

        match self {
            Kind::DeserializeResponse { .. } => {
                return ErrorInfo {
                    retries: None,
                    reason: ErrorReason::Io
                }
            },
            Kind::Wrapped(e) => return e.error_info(),
            Kind::Context { source, .. } => {
                match source.downcast_ref::<Error>() {
                    Some(e) => return e.error_info(),
                    None => {}
                }
            }
            _ => {}
        }

        // Here we go through the chain of type erased errors that led to the current one
        // trying to downcast each to concrete types. We fallback to error string parsing only on
        // private errors (as we don't have the type) and mainly to extract helpfull information
        // on a best effort basis.
        for e in self.chain() {
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
}

pub(crate) struct Chain<'a>(Option<&'a (dyn StdError + 'static)>);

impl<'a> Iterator for Chain<'a> {
    type Item = &'a (dyn StdError + 'static);
    fn next(&mut self) -> Option<Self::Item> {
        match self.0.take() {
            Some(e) => {
                self.0 = e.source();
                Some(e)
            }
            None => None
        }
    }
}

pub type Result<T, E = Error> = core::result::Result<T, E>;

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
            ErrorReason::Code(code) => {
                if (500..600).contains(&code) {
                    all_retries < max_retries && elapsed < retry_timeout
                } else {
                    // TODO manage custom status_code retries
                    false
                }
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

    pub(crate) fn should_retry(&mut self, mut error: Error) -> Result<(Error, ErrorInfo, Duration), Error> {
        let info = error.error_info();
        self.log_attempt(info.clone());
        if self.should_retry_logic() {
            Ok((error, info, self.next_backoff()))
        } else {
            error.metadata = Some(Metadata {
                start: self.start.clone(),
                attempts: self.attempts.clone()
            });
            Err(error)
        }
    }
}
