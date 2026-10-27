//! Magnet URI sanitising.
//!
//! The magnet URI is attacker-controlled input. We parse it ourselves *before*
//! handing anything to librqbit, because `Magnet::parse` expands `so=` ranges
//! into a `Vec<usize>` (memory DoS), and `tr=` trackers make the service issue
//! outbound requests (SSRF). The sanitised URI only contains `xt`, `dn` and
//! public `tr` parameters.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use librqbit::Magnet;
use url::{form_urlencoded, Host, Url};

use crate::error::AppError;

pub const MAX_MAGNET_LEN: usize = 4096;
const MAX_TRACKERS: usize = 16;
const MAX_TRACKER_CANDIDATES: usize = 64;
const MAX_NAME_CHARS: usize = 200;
const DNS_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SanitizedMagnet {
    pub uri: String,
    /// Lowercase hex info-hash (btih).
    pub info_hash: String,
    pub trackers: Vec<String>,
}

pub async fn sanitize_magnet(
    input: &str,
    tracker_allowlist: Option<&[String]>,
) -> Result<SanitizedMagnet, AppError> {
    if input.len() > MAX_MAGNET_LEN {
        return Err(AppError::BadRequest("magnet uri is too long".to_string()));
    }

    let url = Url::parse(input.trim())
        .map_err(|_| AppError::InvalidInput("invalid magnet uri".to_string()))?;
    if url.scheme() != "magnet" {
        return Err(AppError::InvalidInput("invalid magnet uri".to_string()));
    }

    let mut btih: Option<String> = None;
    let mut name: Option<String> = None;
    let mut tracker_candidates: Vec<String> = Vec::new();

    for (key, value) in url.query_pairs() {
        if key.eq_ignore_ascii_case("so") {
            return Err(AppError::BadRequest(
                "selective download (so) is not supported".to_string(),
            ));
        }

        match key.as_ref() {
            "xt" => {
                if btih.is_none() {
                    if let Some(hash) = value.strip_prefix("urn:btih:") {
                        btih = Some(hash.to_string());
                    }
                }
            }
            "dn" => {
                if name.is_none() && !value.is_empty() {
                    name = Some(value.chars().take(MAX_NAME_CHARS).collect());
                }
            }
            "tr" if tracker_candidates.len() < MAX_TRACKER_CANDIDATES => {
                tracker_candidates.push(value.into_owned());
            }
            _ => {}
        }
    }

    let btih =
        btih.ok_or_else(|| AppError::InvalidInput("magnet uri must contain btih".to_string()))?;

    // Validate the hash using librqbit on a minimal URI (no `so`, no trackers).
    let minimal = format!(
        "magnet:?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("xt", &format!("urn:btih:{btih}"))
            .finish()
    );
    let info_hash = Magnet::parse(&minimal)
        .ok()
        .and_then(|magnet| magnet.as_id20())
        .map(|id| id.as_string())
        .ok_or_else(|| AppError::InvalidInput("invalid magnet info-hash".to_string()))?;

    let mut trackers: Vec<String> = Vec::new();
    for candidate in tracker_candidates {
        if trackers.len() >= MAX_TRACKERS {
            break;
        }
        if trackers.contains(&candidate) {
            continue;
        }
        if tracker_allowed(&candidate, tracker_allowlist).await {
            trackers.push(candidate);
        } else {
            tracing::debug!("dropping tracker not allowed by egress policy");
        }
    }

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("xt", &format!("urn:btih:{info_hash}"));
    if let Some(name) = &name {
        serializer.append_pair("dn", name);
    }
    for tracker in &trackers {
        serializer.append_pair("tr", tracker);
    }

    Ok(SanitizedMagnet {
        uri: format!("magnet:?{}", serializer.finish()),
        info_hash,
        trackers,
    })
}

/// Decides whether a tracker URL may be contacted.
///
/// With an allowlist, only listed hosts (exact, case-insensitive) pass and no
/// DNS check is made. Without one, the scheme must be http/https/udp and the
/// host (literal IP or every resolved address) must be public.
pub async fn tracker_allowed(raw: &str, allowlist: Option<&[String]>) -> bool {
    let Ok(url) = Url::parse(raw) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https" | "udp") {
        return false;
    }
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    let Some(host) = url.host() else {
        return false;
    };

    if let Some(allowlist) = allowlist {
        let host_str = match &host {
            Host::Domain(domain) => domain.to_ascii_lowercase(),
            Host::Ipv4(ip) => ip.to_string(),
            Host::Ipv6(ip) => ip.to_string(),
        };
        return allowlist.contains(&host_str);
    }

    match host {
        Host::Ipv4(ip) => is_public_ip(IpAddr::V4(ip)),
        Host::Ipv6(ip) => is_public_ip(IpAddr::V6(ip)),
        Host::Domain(domain) => {
            let domain = domain.trim_end_matches('.').to_ascii_lowercase();
            if domain.is_empty() || is_local_hostname(&domain) {
                return false;
            }
            let port = url.port_or_known_default().unwrap_or(80);
            let lookup = tokio::time::timeout(
                DNS_TIMEOUT,
                tokio::net::lookup_host((domain.as_str(), port)),
            )
            .await;
            match lookup {
                Ok(Ok(addrs)) => {
                    let mut any = false;
                    for addr in addrs {
                        any = true;
                        if !is_public_ip(addr.ip()) {
                            return false;
                        }
                    }
                    any
                }
                _ => false,
            }
        }
    }
}

fn is_local_hostname(domain: &str) -> bool {
    domain == "localhost"
        || !domain.contains('.')
        || [
            ".localhost",
            ".local",
            ".internal",
            ".localdomain",
            ".lan",
            ".home.arpa",
        ]
        .iter()
        .any(|suffix| domain.ends_with(suffix))
}

pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 198 && (b == 18 || b == 19))
        || a >= 240)
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return false;
    }
    let segments = ip.segments();
    // unique-local fc00::/7, link-local fe80::/10
    if (segments[0] & 0xfe00) == 0xfc00 || (segments[0] & 0xffc0) == 0xfe80 {
        return false;
    }
    // documentation 2001:db8::/32
    if segments[0] == 0x2001 && segments[1] == 0x0db8 {
        return false;
    }
    // IPv4-mapped (::ffff:a.b.c.d) and NAT64 (64:ff9b::/96): check embedded v4
    let embedded =
        |hi: u16, lo: u16| Ipv4Addr::new((hi >> 8) as u8, hi as u8, (lo >> 8) as u8, lo as u8);
    if segments[..5] == [0, 0, 0, 0, 0] && segments[5] == 0xffff {
        return is_public_v4(embedded(segments[6], segments[7]));
    }
    if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2..6] == [0, 0, 0, 0] {
        return is_public_v4(embedded(segments[6], segments[7]));
    }
    // IPv4-compatible (::a.b.c.d)
    if segments[..6] == [0, 0, 0, 0, 0, 0] {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn base() -> String {
        format!("magnet:?xt=urn:btih:{HASH}")
    }

    #[tokio::test]
    async fn rejects_select_only_parameter() {
        let uri = format!("{}&so=0-4294967295", base());
        let error = sanitize_magnet(&uri, None).await.unwrap_err();
        assert!(matches!(error, AppError::BadRequest(_)));
    }

    #[tokio::test]
    async fn rejects_oversized_magnet() {
        let uri = format!("{}&dn={}", base(), "a".repeat(MAX_MAGNET_LEN));
        let error = sanitize_magnet(&uri, None).await.unwrap_err();
        assert!(matches!(error, AppError::BadRequest(_)));
    }

    #[tokio::test]
    async fn rejects_non_magnet_and_missing_hash() {
        assert!(sanitize_magnet("http://example.com", None).await.is_err());
        assert!(sanitize_magnet("magnet:?dn=x", None).await.is_err());
        assert!(sanitize_magnet("magnet:?xt=urn:btih:zzzz", None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn keeps_public_trackers_and_drops_internal_ones() {
        let uri = format!(
            "{}&dn=Book&tr=udp://8.8.8.8:6969/announce&tr=http://127.0.0.1:8080/a&tr=http://10.0.0.5/a&tr=http://192.168.1.1/a&tr=http://169.254.169.254/latest&tr=http://[::1]/a&tr=http://[fd00::1]/a&tr=http://[fe80::1]/a&tr=file:///etc/passwd&tr=http://localhost/a&tr=https://1.1.1.1/announce",
            base()
        );
        let sanitized = sanitize_magnet(&uri, None).await.unwrap();

        assert_eq!(sanitized.info_hash, HASH);
        assert_eq!(
            sanitized.trackers,
            vec![
                "udp://8.8.8.8:6969/announce".to_string(),
                "https://1.1.1.1/announce".to_string()
            ]
        );
        assert!(sanitized.uri.starts_with("magnet:?xt=urn%3Abtih%3A"));
        assert!(!sanitized.uri.contains("127.0.0.1"));
        assert!(Magnet::parse(&sanitized.uri).is_ok());
    }

    #[tokio::test]
    async fn allowlist_drops_everything_else() {
        let allowlist = vec!["tracker.example.org".to_string()];
        let uri = format!(
            "{}&tr=udp://tracker.example.org:80/announce&tr=udp://8.8.8.8:80/announce",
            base()
        );
        let sanitized = sanitize_magnet(&uri, Some(&allowlist)).await.unwrap();
        assert_eq!(
            sanitized.trackers,
            vec!["udp://tracker.example.org:80/announce".to_string()]
        );
    }

    #[test]
    fn classifies_ip_addresses() {
        for ip in [
            "0.0.0.0",
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "100.64.0.1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_public_ip(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
            assert!(is_public_ip(ip.parse().unwrap()), "{ip}");
        }
    }
}
