use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::Result;

pub const MAX_DOWNLOAD_TTL_SECS: i64 = 365 * 86_400;
pub const MAX_CLEANUP_INTERVAL_SECS: u64 = 86_400;
pub const MAX_CONCURRENT_DOWNLOADS_LIMIT: usize = 64;
pub const MAX_TIMEOUT_SECS: u64 = 86_400;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub storage_root: PathBuf,
    pub api_key: String,
    pub download_ttl_secs: i64,
    pub cleanup_interval_secs: u64,
    pub max_concurrent_downloads: usize,
    pub metadata_timeout_secs: u64,
    pub no_progress_timeout_secs: u64,
    pub max_torrent_bytes: Option<u64>,
    pub tracker_allowlist: Option<Vec<String>>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("APP_BIND_ADDR")
            .unwrap_or_else(|_| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000).to_string())
            .parse()?;
        let database_url = env::var("APP_DATABASE_URL")
            .unwrap_or_else(|_| "sqlite://audiobooks_downloader.db".to_string());
        let storage_root = env::var("APP_STORAGE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./storage"));
        let api_key = parse_required_env(env::var("APP_API_KEY").ok(), "APP_API_KEY")?;
        let download_ttl_secs = parse_ttl_secs(env::var("APP_DOWNLOAD_TTL_SECS").ok())?;
        let cleanup_interval_secs = parse_bounded_env_or_default(
            env::var("APP_CLEANUP_INTERVAL_SECS").ok(),
            "APP_CLEANUP_INTERVAL_SECS",
            60,
            1,
            MAX_CLEANUP_INTERVAL_SECS,
        )?;
        let max_concurrent_downloads = parse_bounded_env_or_default(
            env::var("MAX_CONCURRENT_DOWNLOADS").ok(),
            "MAX_CONCURRENT_DOWNLOADS",
            3_usize,
            1,
            MAX_CONCURRENT_DOWNLOADS_LIMIT,
        )?;
        let metadata_timeout_secs = parse_bounded_env_or_default(
            env::var("METADATA_TIMEOUT_SECS").ok(),
            "METADATA_TIMEOUT_SECS",
            600,
            1,
            MAX_TIMEOUT_SECS,
        )?;
        let no_progress_timeout_secs = parse_bounded_env_or_default(
            env::var("NO_PROGRESS_TIMEOUT_SECS").ok(),
            "NO_PROGRESS_TIMEOUT_SECS",
            600,
            1,
            MAX_TIMEOUT_SECS,
        )?;
        let max_torrent_bytes = match env::var("MAX_TORRENT_BYTES").ok() {
            Some(value) => Some(parse_bounded_env_or_default(
                Some(value),
                "MAX_TORRENT_BYTES",
                1_u64,
                1,
                i64::MAX as u64,
            )?),
            None => None,
        };
        let tracker_allowlist = parse_allowlist(env::var("TRACKER_ALLOWLIST").ok());

        Ok(Self {
            bind_addr,
            database_url,
            storage_root,
            api_key,
            download_ttl_secs,
            cleanup_interval_secs,
            max_concurrent_downloads,
            metadata_timeout_secs,
            no_progress_timeout_secs,
            max_torrent_bytes,
            tracker_allowlist,
        })
    }
}

fn parse_required_env(value: Option<String>, key: &str) -> Result<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(value),
        Some(_) => Err(anyhow::anyhow!("invalid {key}: must not be empty")),
        None => Err(anyhow::anyhow!("missing {key}")),
    }
}

fn parse_env_or_default<T>(value: Option<String>, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match value {
        Some(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid {key}: {error}")),
        None => Ok(default),
    }
}

fn parse_bounded_env_or_default<T>(
    value: Option<String>,
    key: &str,
    default: T,
    min: T,
    max: T,
) -> Result<T>
where
    T: std::str::FromStr + PartialOrd + std::fmt::Display + Copy,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = parse_env_or_default(value, key, default)?;
    if value < min || value > max {
        return Err(anyhow::anyhow!(
            "invalid {key}: must be between {min} and {max}"
        ));
    }

    Ok(value)
}

pub fn parse_ttl_secs(value: Option<String>) -> Result<i64> {
    parse_bounded_env_or_default(
        value,
        "APP_DOWNLOAD_TTL_SECS",
        86_400,
        1,
        MAX_DOWNLOAD_TTL_SECS,
    )
}

fn parse_allowlist(value: Option<String>) -> Option<Vec<String>> {
    let hosts: Vec<String> = value?
        .split(',')
        .map(|host| host.trim().to_ascii_lowercase())
        .filter(|host| !host.is_empty())
        .collect();

    (!hosts.is_empty()).then_some(hosts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_required_env_accepts_present_value() {
        assert_eq!(
            parse_required_env(Some("secret".to_string()), "APP_API_KEY").unwrap(),
            "secret"
        );
    }

    #[test]
    fn parse_required_env_rejects_missing_value() {
        assert!(parse_required_env(None, "APP_API_KEY").is_err());
    }

    #[test]
    fn parse_required_env_rejects_empty_value() {
        assert!(parse_required_env(Some("   ".to_string()), "APP_API_KEY").is_err());
    }

    #[test]
    fn parse_env_or_default_uses_default_when_missing() {
        assert_eq!(
            parse_env_or_default::<i64>(None, "APP_DOWNLOAD_TTL_SECS", 86_400).unwrap(),
            86_400
        );
    }

    #[test]
    fn parse_env_or_default_parses_present_value() {
        assert_eq!(
            parse_env_or_default(Some("120".to_string()), "APP_DOWNLOAD_TTL_SECS", 86_400).unwrap(),
            120
        );
    }

    #[test]
    fn parse_env_or_default_rejects_invalid_value() {
        assert!(parse_env_or_default::<u64>(
            Some("bad".to_string()),
            "APP_CLEANUP_INTERVAL_SECS",
            60
        )
        .is_err());
    }

    #[test]
    fn bounded_rejects_zero_value() {
        assert!(parse_bounded_env_or_default(
            Some("0".to_string()),
            "APP_CLEANUP_INTERVAL_SECS",
            60_u64,
            1,
            MAX_CLEANUP_INTERVAL_SECS
        )
        .is_err());
    }

    #[test]
    fn ttl_defaults_and_validates_bounds() {
        assert_eq!(parse_ttl_secs(None).unwrap(), 86_400);
        assert_eq!(parse_ttl_secs(Some("1".to_string())).unwrap(), 1);
        assert!(parse_ttl_secs(Some("0".to_string())).is_err());
        assert!(parse_ttl_secs(Some("-5".to_string())).is_err());
        assert!(parse_ttl_secs(Some(i64::MAX.to_string())).is_err());
        assert!(parse_ttl_secs(Some((MAX_DOWNLOAD_TTL_SECS + 1).to_string())).is_err());
        assert!(parse_ttl_secs(Some("abc".to_string())).is_err());
    }

    #[test]
    fn allowlist_is_normalised_and_empty_means_unset() {
        assert_eq!(
            parse_allowlist(Some(" A.example.org, ,b.example.org ".to_string())),
            Some(vec![
                "a.example.org".to_string(),
                "b.example.org".to_string()
            ])
        );
        assert_eq!(parse_allowlist(Some(" , ".to_string())), None);
        assert_eq!(parse_allowlist(None), None);
    }
}
