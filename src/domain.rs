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

pub fn compute_expires_at(completed_at: DateTime<Utc>, ttl_secs: i64) -> DateTime<Utc> {
    completed_at + Duration::seconds(ttl_secs)
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
            completed_at + Duration::seconds(300)
        );
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
