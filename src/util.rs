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

/// 官方禁用符里、换成全角后不会再触发 14014 的那一组。
/// `. : ,` 不在这里：它们的全角形属于兼容区，内容审核 NFKC 之后仍是半角。
fn quark_forbidden_pairs() -> &'static [(char, char)] {
    &[
        ('\\', '\u{FF3C}'), // ＼
        ('*', '\u{FF0A}'),  // ＊
        ('?', '\u{FF1F}'),  // ？
        ('"', '\u{FF02}'),  // ＂
        ('\'', '\u{FF07}'), // ＇
        ('<', '\u{FF1C}'),  // ＜
        ('>', '\u{FF1E}'),  // ＞
        ('|', '\u{FF5C}'),  // ｜
        ('%', '\u{FF05}'),  // ％
        ('&', '\u{FF06}'),  // ＆
        ('#', '\u{FF03}'),  // ＃
        (';', '\u{FF1B}'),  // ；
        // 路径段里一般不会出现 `/`；若出现同样编码，避免 Open API 直接拒。
        ('/', '\u{FF0F}'), // ／
    ]
}

/// 间隔号。中文文件名里合法，用来给「审核会拒的标点」当转义前缀。
const MIDDOT: char = '\u{00B7}';

/// 会触发 illegal text 的标点 → 间隔号后面的标记。
/// 标记用全角拉丁字母：NFKC 后是普通字母，不会折回 `. : ,`。
/// 半角与全角各用不同标记，避免「主题四：…」「班，…」和 ASCII 标点互相串名。
fn punct_mark(ch: char) -> Option<char> {
    Some(match ch {
        '.' => '\u{FF44}',        // ｄ
        '\u{FF0E}' => '\u{FF45}', // ｅ 原文全角句点，不能再原样落盘
        ':' => '\u{FF43}',        // ｃ
        '\u{FF1A}' => '\u{FF46}', // ｆ 原文全角冒号（如「主题四：我们都是好朋友」）
        ',' => '\u{FF4D}',        // ｍ
        '\u{FF0C}' => '\u{FF4E}', // ｎ 原文全角逗号（如「班，家长活动2017.9」）
        // 顿号不折成半角逗号，但内容审核同样判 illegal text。
        '\u{3001}' => '\u{FF4B}', // ｋ 如「卫生保健计划、总结2018.02」
        _ => return None,
    })
}

/// `punct_mark` 的逆。只认全角字母，避免把「笔记·docx」里的 ASCII d 当成标记。
fn punct_from_mark(mark: char) -> Option<char> {
    Some(match mark {
        '\u{FF44}' => '.',
        '\u{FF45}' => '\u{FF0E}',
        '\u{FF43}' => ':',
        '\u{FF46}' => '\u{FF1A}',
        '\u{FF4D}' => ',',
        '\u{FF4E}' => '\u{FF0C}',
        '\u{FF4B}' => '\u{3001}',
        _ => return None,
    })
}

/// 其余禁用符、弯引号、控制符。不处理 `. : ,` 和间隔号。
fn map_other_encode(ch: char) -> char {
    for &(from, to) in quark_forbidden_pairs() {
        if ch == from {
            return to;
        }
    }
    match ch {
        // 弯引号按直引号收成全角，避免 HTML 转义把名字改掉。
        '\u{2018}' | '\u{2019}' | '\u{201A}' | '\u{FF07}' => '\u{FF07}',
        '\u{201C}' | '\u{201D}' | '\u{201E}' => '\u{FF02}',
        // C0 控制符不能进文件名。
        c if c.is_control() => '_',
        c => c,
    }
}

/// 列表读回：旧版本直接落盘的全角 `．：，` 仍还原成半角，已同步的文件不用改名。
fn map_other_decode(ch: char) -> char {
    match ch {
        '\u{FF0E}' => return '.',
        '\u{FF1A}' => return ':',
        '\u{FF0C}' => return ',',
        _ => {}
    }
    for &(from, to) in quark_forbidden_pairs() {
        if ch == to {
            return from;
        }
    }
    ch
}

/// 写入夸克前编码文件/目录名。
/// `. : ,`、顿号 `、` 及中文全角冒号/逗号改成「间隔号 + 全角字母」。
/// 全角 `．：，` 会被折回半角，顿号本身也会被判 illegal text。
/// 已是该形式的片段原样保留，建目录/上传路径上的二次编码不会把名字再改一层。
pub fn quark_encode_name(name: &str) -> String {
    let trimmed = name.trim_matches(|c: char| c == ' ' || c == '\t');
    let chars: Vec<char> = trimmed.chars().collect();
    let mut out = String::with_capacity(trimmed.len() + 8);
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == MIDDOT {
            if let Some(nxt) = chars.get(i + 1).copied() {
                // 二次编码：间隔号本身、或已经带标记的标点，都不再展开。
                if nxt == MIDDOT || punct_from_mark(nxt).is_some() {
                    out.push(MIDDOT);
                    out.push(nxt);
                    i += 2;
                    continue;
                }
            }
            // 原文里的间隔号（姓名里常见）写成两个，解码时再折回一个。
            out.push(MIDDOT);
            out.push(MIDDOT);
            i += 1;
            continue;
        }
        if let Some(mark) = punct_mark(ch) {
            out.push(MIDDOT);
            out.push(mark);
            i += 1;
            continue;
        }
        out.push(map_other_encode(ch));
        i += 1;
    }
    if out.is_empty() {
        // 全是空白时给一个可落盘占位，避免 Open 空名。
        out.push('_');
    }
    out
}

/// 从夸克列表读回后解码，使 PROPFIND 与客户端路径一致。
pub fn quark_decode_name(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len());
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == MIDDOT {
            if let Some(nxt) = chars.get(i + 1).copied() {
                if nxt == MIDDOT {
                    out.push(MIDDOT);
                    i += 2;
                    continue;
                }
                if let Some(orig) = punct_from_mark(nxt) {
                    out.push(orig);
                    i += 2;
                    continue;
                }
            }
            // 网盘上本来就有的单个间隔号，不是我们写的转义。
            out.push(MIDDOT);
            i += 1;
            continue;
        }
        out.push(map_other_decode(ch));
        i += 1;
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
        // ＊ 仍是全角星号；点号改成间隔号 + 全角 ｄ，避免 NFKC 折回半角点。
        assert_eq!(enc, "＊\u{00B7}\u{FF44}poiig\u{00B7}\u{FF44}top_ecc");
        assert_ne!(enc, raw);
        assert_eq!(quark_decode_name(&enc), raw);
        assert_eq!(quark_encode_name(&enc), enc);
        assert_eq!(quark_encode_name("normal.txt"), "normal\u{00B7}\u{FF44}txt");
        // 旧落盘名（全角句点）仍能还原，已同步文件不用重传。
        assert_eq!(quark_decode_name("normal．txt"), "normal.txt");
        assert_eq!(quark_decode_name("＊．poiig．top_ecc"), raw);
    }

    #[test]
    fn test_quark_name_encode_dot_date_folder() {
        // 复现：家委会工作2020.09 → Open API illegal text (errno=14014)
        let raw = "家委会工作2020.09";
        let enc = quark_encode_name(raw);
        assert_eq!(enc, "家委会工作2020\u{00B7}\u{FF44}09");
        assert_eq!(quark_decode_name(&enc), raw);
        assert_eq!(quark_encode_name(&enc), enc);
    }

    #[test]
    fn test_illegal_text_reported_folder_names() {
        // 群晖同步仍 14014 的目录段：半角点、原文里的全角冒号/逗号。
        let samples = [
            "2018.09教育教学资料",
            "教师备课2018.09",
            "中2班2018.09",
            "蔡叶婷备课",
            "主题四：我们都是好朋友",
            "2017.9教育教学资料",
            "家长工作2017.9",
            "班，家长活动2017.9",
            "中2",
            "教研2017.9",
            "区域活动材料记录2017.9",
            "大2班",
            "2020.9中心资料",
            "缴费2020.9",
            "天怡2020.9",
            "中心",
            "家长工作2020.09",
            "家委会工作2020.09",
            "a:b,c",
            "阿卜杜拉·买买提",
            "笔记·docx",
            "2017-2018下",
            "卫生保健计划、总结2018.02",
        ];
        for raw in samples {
            let enc = quark_encode_name(raw);
            assert_eq!(quark_decode_name(&enc), raw, "roundtrip {raw}");
            assert_eq!(quark_encode_name(&enc), enc, "idempotent {raw}");
            assert!(
                !enc.chars().any(|c| {
                    matches!(
                        c,
                        '.' | ':' | ',' | '\u{FF0E}' | '\u{FF1A}' | '\u{FF0C}' | '\u{3001}'
                    )
                }),
                "仍含会被拒的标点: {raw} -> {enc}"
            );
        }
        assert_eq!(
            quark_encode_name("卫生保健计划、总结2018.02"),
            "卫生保健计划\u{00B7}\u{FF4B}总结2018\u{00B7}\u{FF44}02"
        );
        assert_eq!(
            quark_encode_name("主题四：我们都是好朋友"),
            "主题四\u{00B7}\u{FF46}我们都是好朋友"
        );
        assert_eq!(
            quark_encode_name("班，家长活动2017.9"),
            "班\u{00B7}\u{FF4E}家长活动2017\u{00B7}\u{FF44}9"
        );
        // 旧编码把 ASCII 冒号/逗号存成全角，列表仍还原成半角。
        assert_eq!(quark_decode_name("a：b，c"), "a:b,c");
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
