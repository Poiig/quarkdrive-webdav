//! 通用小工具：人类可读容量解析等。

use anyhow::{bail, Result};
use chrono::{DateTime, FixedOffset, Utc};

/// 中国标准时间 UTC+8（Asia/Shanghai，无夏令时）。
pub fn shanghai_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("UTC+8")
}

/// 群晖加密任务在远端创建的元数据目录名（须真实 MKCOL/上传，不能吞掉）。
pub const CLOUDSYNC_METADATA_DIR: &str = "@SynologyCloudSync";

pub fn is_cloudsync_metadata_dir(name: &str) -> bool {
    name == CLOUDSYNC_METADATA_DIR
}

/// 路径是否在 `@SynologyCloudSync` 之下（该树下文件一律要落盘）。
pub fn is_under_cloudsync_metadata(dav_path: &str) -> bool {
    let norm = dav_path.trim().replace('\\', "/");
    norm.split('/')
        .any(|seg| !seg.is_empty() && is_cloudsync_metadata_dir(seg))
}

/// 系统/同步垃圾文件：对齐 OpenList `IsSystemFile`，并补充群晖常见项。
/// 对普通用户目录下的这些名字 PUT/MKCOL 做本地成功、不上云。
pub fn is_ignored_system_file(name: &str) -> bool {
    match name {
        ".DS_Store" | "desktop.ini" | "Thumbs.db" | "@eaDir" | "@tmp" | "#recycle"
        | "#snapshot" | ".SynologyWorkingDirectory" | "@sharebin" => true,
        _ => {
            name.starts_with("._") || name.starts_with("~.")
        }
    }
}

/// WebDAV 路径 + 文件名是否应忽略上传/建目录（元数据目录例外）。
pub fn should_ignore_system_path(dav_path: &str, file_name: &str) -> bool {
    if is_cloudsync_metadata_dir(file_name) || is_under_cloudsync_metadata(dav_path) {
        return false;
    }
    is_ignored_system_file(file_name)
}

/// 当前 Asia/Shanghai 时间，不依赖容器 TZ。
pub fn now_shanghai() -> DateTime<FixedOffset> {
    Utc::now().with_timezone(&shanghai_offset())
}

/// 把字节数格式化为可读容量，如 `1.5 MB`、`320 KB`。
pub fn format_bytes(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if size >= TB {
        format!("{:.1} TB", size as f64 / TB as f64)
    } else if size >= GB {
        format!("{:.1} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.1} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.1} KB", size as f64 / KB as f64)
    } else {
        format!("{size} B")
    }
}

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
    fn test_shanghai_offset_is_utc8() {
        let now = now_shanghai();
        assert_eq!(now.offset().local_minus_utc(), 8 * 3600);
    }

    #[test]
    fn test_ignored_system_files() {
        assert!(is_ignored_system_file(".DS_Store"));
        assert!(is_ignored_system_file("@eaDir"));
        assert!(is_ignored_system_file("._photo.jpg"));
        assert!(is_ignored_system_file(".SynologyWorkingDirectory"));
        assert!(!is_ignored_system_file("cloudsync_encrypt.info"));
        assert!(!is_ignored_system_file("@SynologyCloudSync"));
        assert!(!is_ignored_system_file("photo.jpg"));
        assert!(!is_ignored_system_file("readme.md"));
        assert!(should_ignore_system_path("/photos", ".DS_Store"));
        assert!(!should_ignore_system_path("/@SynologyCloudSync", "cloudsync_encrypt.info"));
        assert!(!should_ignore_system_path("/homes/@SynologyCloudSync/x", "cloudsync_encrypt.info"));
        assert!(is_under_cloudsync_metadata("/homes/@SynologyCloudSync/cloudsync_encrypt.info"));
    }

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

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(64 * 1024 * 1024), "64.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }
}
