use atlas_metrics::metrics::MetricKind;
use atlas_metrics::{MetricLevel, MetricRegistry};

pub(crate) const RQ_SEND_TIME: &str = "REQUEST_SEND_TIME";
pub(crate) const RQ_SEND_TIME_ID: usize = 1000;

pub(crate) const OUTGOING_MESSAGE_SIZE: &str = "OUTGOING_MESSAGE_SIZE";
pub(crate) const OUTGOING_MESSAGE_SIZE_ID: usize = 1001;

pub(crate) const INCOMING_MESSAGE_SIZE: &str = "INCOMING_MESSAGE_SIZE";
pub(crate) const INCOMING_MESSAGE_SIZE_ID: usize = 1002;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (
            RQ_SEND_TIME_ID,
            RQ_SEND_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Trace,
        )
            .into(),
        (
            OUTGOING_MESSAGE_SIZE_ID,
            OUTGOING_MESSAGE_SIZE.to_string(),
            MetricKind::CountMax(10),
            MetricLevel::Trace,
        )
            .into(),
        (
            INCOMING_MESSAGE_SIZE_ID,
            INCOMING_MESSAGE_SIZE.to_string(),
            MetricKind::CountMax(10),
            MetricLevel::Trace,
        )
            .into(),
    ]
}
