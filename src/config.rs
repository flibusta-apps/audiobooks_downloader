use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::Result;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub storage_root: PathBuf,
    pub api_key: String,
    pub download_ttl_secs: i64,
    pub cleanup_interval_secs: u64,
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
        let download_ttl_secs = parse_env_or_default(
            env::var("APP_DOWNLOAD_TTL_SECS").ok(),
            "APP_DOWNLOAD_TTL_SECS",
            86_400,
        )?;
        let cleanup_interval_secs = parse_nonzero_env_or_default(
            env::var("APP_CLEANUP_INTERVAL_SECS").ok(),
            "APP_CLEANUP_INTERVAL_SECS",
            60,
        )?;

        Ok(Self {
            bind_addr,
            database_url,
            storage_root,
            api_key,
            download_ttl_secs,
            cleanup_interval_secs,
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

fn parse_nonzero_env_or_default(value: Option<String>, key: &str, default: u64) -> Result<u64> {
    let value = parse_env_or_default(value, key, default)?;
    if value == 0 {
        return Err(anyhow::anyhow!("invalid {key}: must be greater than 0"));
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{parse_env_or_default, parse_nonzero_env_or_default, parse_required_env};

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
    fn parse_nonzero_env_or_default_rejects_zero_value() {
        assert!(parse_nonzero_env_or_default(
            Some("0".to_string()),
            "APP_CLEANUP_INTERVAL_SECS",
            60
        )
        .is_err());
    }
}
