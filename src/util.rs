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

/// 夸克禁止/易触发 `illegal text`(14014) 的字符 → 全角可逆替换。
/// WebDAV/群晖仍见原名；落盘用编码名。含官方禁用符，以及 `.` `'` `&` 等实测会拒的字符。
fn quark_forbidden_pairs() -> &'static [(char, char)] {
    &[
        ('\\', '\u{FF3C}'), // ＼
        (':', '\u{FF1A}'),  // ：
        ('*', '\u{FF0A}'),  // ＊
        ('?', '\u{FF1F}'),  // ？
        ('"', '\u{FF02}'),  // ＂
        ('\'', '\u{FF07}'), // ＇
        ('<', '\u{FF1C}'),  // ＜
        ('>', '\u{FF1E}'),  // ＞
        ('|', '\u{FF5C}'),  // ｜
        (',', '\u{FF0C}'),  // ，
        ('%', '\u{FF05}'),  // ％
        ('&', '\u{FF06}'),  // ＆
        ('#', '\u{FF03}'),  // ＃
        (';', '\u{FF1B}'),  // ；
        // Open `dir_path` / 内容审核对半角 `.` 偶发 illegal text（如「家委会工作2020.09」）。
        ('.', '\u{FF0E}'), // ．
        // 路径段里一般不会出现 `/`；若出现同样编码，避免 Open API 直接拒。
        ('/', '\u{FF0F}'), // ／
    ]
}

fn map_quark_char(ch: char, encode: bool) -> Option<char> {
    for &(from, to) in quark_forbidden_pairs() {
        if encode && ch == from {
            return Some(to);
        }
        if !encode && ch == to {
            return Some(from);
        }
    }
    // 弯引号等常见变体也按直引号处理（编码时 → 全角＇，解码时 → ASCII '）。
    if encode {
        match ch {
            '\u{2018}' | '\u{2019}' | '\u{201A}' | '\u{FF07}' => Some('\u{FF07}'),
            '\u{201C}' | '\u{201D}' | '\u{201E}' => Some('\u{FF02}'),
            // C0 控制符不能进文件名
            c if c.is_control() => Some('_'),
            _ => None,
        }
    } else {
        None
    }
}

/// 写入夸克前编码文件/目录名。
pub fn quark_encode_name(name: &str) -> String {
    let trimmed = name.trim_matches(|c: char| c == ' ' || c == '\t');
    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        out.push(map_quark_char(ch, true).unwrap_or(ch));
    }
    if out.is_empty() {
        // 全是空白时给一个可落盘占位，避免 Open 空名。
        out.push('_');
    }
    out
}

/// 从夸克列表读回后解码，使 PROPFIND 与客户端路径一致。
pub fn quark_decode_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        out.push(map_quark_char(ch, false).unwrap_or(ch));
    }
    out
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
    fn test_cloudsync_metadata_dir_name() {
        assert!(is_cloudsync_metadata_dir(CLOUDSYNC_METADATA_DIR));
        assert!(!is_cloudsync_metadata_dir("@sharebin"));
    }

    #[test]
    fn test_quark_name_encode_roundtrip_star() {
        let raw = "*.poiig.top_ecc";
        let enc = quark_encode_name(raw);
        assert_eq!(enc, "＊．poiig．top_ecc");
        assert_ne!(enc, raw);
        assert_eq!(quark_decode_name(&enc), raw);
        assert_eq!(quark_encode_name("normal.txt"), "normal．txt");
        assert_eq!(quark_decode_name("normal．txt"), "normal.txt");
    }

    #[test]
    fn test_quark_name_encode_dot_date_folder() {
        // 复现：家委会工作2020.09 → Open API illegal text (errno=14014)
        let raw = "家委会工作2020.09";
        let enc = quark_encode_name(raw);
        assert_eq!(enc, "家委会工作2020．09");
        assert_eq!(quark_decode_name(&enc), raw);
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
