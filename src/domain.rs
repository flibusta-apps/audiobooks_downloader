use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    PendingMetadata,
    Downloading,
    Completed,
    Failed,
    Interrupted,
    Expired,
}

impl DownloadStatus {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::PendingMetadata => "pending_metadata",
            Self::Downloading => "downloading",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
            Self::Expired => "expired",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, &'static str> {
        match value {
            "pending_metadata" => Ok(Self::PendingMetadata),
            "downloading" => Ok(Self::Downloading),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            "expired" => Ok(Self::Expired),
            _ => Err("unknown download status"),
        }
    }
}

/// Returns `None` if the TTL is non-positive or the arithmetic overflows.
pub fn compute_expires_at(completed_at: DateTime<Utc>, ttl_secs: i64) -> Option<DateTime<Utc>> {
    if ttl_secs <= 0 {
        return None;
    }

    completed_at.checked_add_signed(Duration::try_seconds(ttl_secs)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    #[test]
    fn computes_expiration_from_completion_time() {
        let completed_at = Utc::now();
        assert_eq!(
            compute_expires_at(completed_at, 300),
            Some(completed_at + Duration::seconds(300))
        );
    }

    #[test]
    fn expiration_arithmetic_is_checked() {
        let now = Utc::now();
        assert_eq!(compute_expires_at(now, i64::MAX), None);
        assert_eq!(compute_expires_at(now, 0), None);
        assert_eq!(compute_expires_at(now, -1), None);
    }

    #[test]
    fn roundtrips_download_status_values() {
        let cases = [
            (DownloadStatus::PendingMetadata, "pending_metadata"),
            (DownloadStatus::Downloading, "downloading"),
            (DownloadStatus::Completed, "completed"),
            (DownloadStatus::Failed, "failed"),
            (DownloadStatus::Interrupted, "interrupted"),
            (DownloadStatus::Expired, "expired"),
        ];

        for (status, db_value) in cases {
            assert_eq!(DownloadStatus::from_db(db_value).unwrap(), status);
            assert_eq!(status.as_db(), db_value);
        }
    }

    #[test]
    fn rejects_unknown_download_status_values() {
        assert!(DownloadStatus::from_db("unknown").is_err());
    }
}
