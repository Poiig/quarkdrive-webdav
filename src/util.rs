//! 通用小工具：人类可读容量解析等。

use anyhow::{bail, Result};

/// 解析 `64MB` / `1GB` / `512MiB` / 纯数字（按字节）为 u64。
pub fn parse_byte_size(input: &str) -> Result<u64> {
    let s = input.trim().replace('_', "").replace(' ', "");
    if s.is_empty() {
        bail!("容量字符串为空");
    }
    if let Ok(n) = s.parse::<u64>() {
        return Ok(n);
    }
    let upper = s.to_ascii_uppercase();
    let (num_str, mult) = if let Some(rest) = upper.strip_suffix("GIB") {
        (rest, 1024u64.pow(3))
    } else if let Some(rest) = upper.strip_suffix("MIB") {
        (rest, 1024u64.pow(2))
    } else if let Some(rest) = upper.strip_suffix("KIB") {
        (rest, 1024u64)
    } else if let Some(rest) = upper.strip_suffix("GB") {
        (rest, 1024u64.pow(3))
    } else if let Some(rest) = upper.strip_suffix("MB") {
        (rest, 1024u64.pow(2))
    } else if let Some(rest) = upper.strip_suffix("KB") {
        (rest, 1024u64)
    } else if let Some(rest) = upper.strip_suffix('G') {
        (rest, 1024u64.pow(3))
    } else if let Some(rest) = upper.strip_suffix('M') {
        (rest, 1024u64.pow(2))
    } else if let Some(rest) = upper.strip_suffix('K') {
        (rest, 1024u64)
    } else if let Some(rest) = upper.strip_suffix('B') {
        (rest, 1u64)
    } else {
        bail!("无法解析容量: {input}（支持如 64MB、1GB、512MiB）");
    };
    let num: f64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("无法解析容量数字: {input}"))?;
    if num < 0.0 {
        bail!("容量不能为负: {input}");
    }
    Ok((num * mult as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_byte_size() {
        assert_eq!(parse_byte_size("1024").unwrap(), 1024);
        assert_eq!(parse_byte_size("64MB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_byte_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_byte_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("1.5GB").unwrap(), (1.5 * 1024.0 * 1024.0 * 1024.0) as u64);
        assert_eq!(parse_byte_size("0").unwrap(), 0);
    }
}
