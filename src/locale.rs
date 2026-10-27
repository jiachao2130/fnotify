// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! 中英双语消息（i18n）。翻译范围：
//! 仅 fnotify 自有消息（help/usage 横幅 + 错误消息模板）跟随 locale；
//! OS/strerror 动态文本（io::Error 的 Display，如 "Permission denied"）
//! **始终保持英文**——Rust 不做 setlocale，而且那属于 OS 的输出。
//!
//! locale 检测（优先级，进程内 OnceLock 缓存一次）：`FNOTIFY_LANG`（显式覆盖：
//! trim 后 zh/en 大小写不敏感；空/未设/其他值 → 跳过走标准链）优先于标准链
//! `LC_ALL` → `LC_MESSAGES` → `LANG`；取到的值去首尾空白后以 zh 开头（大小写
//! 不敏感，如 zh_CN.UTF-8）→ 中文；空/未设/C/POSIX/en* 等一律英文。
//!
//! 翻译通道：
//! - `pick`：整句静态模板二选一（help/usage 横幅、无占位符消息）。
//! - `pickf!` 宏：带占位符的模板分叉。format! 的格式串必须是字面量，
//!   `pick` 的返回值进不了 format!，故格式化站点用宏（en/zh 两个字面量
//!   模板占位符逐一同序，按 locale 二选一后插入同一组实参）。
//!
//! 标点约定：中文文案括号/分号全角；冒号一律半角加空格——其后均为 ASCII 内容
//! （用法串/占位符/路径/strerror 续文）。

use std::sync::OnceLock;

/// locale 是否为中文（检测 + 缓存；env 读取只做一次，单测走纯函数 decide）。
pub fn is_zh() -> bool {
    static ZH: OnceLock<bool> = OnceLock::new();
    *ZH.get_or_init(detect_zh)
}

fn detect_zh() -> bool {
    let get = |key: &str| std::env::var(key).ok();
    decide(
        get("FNOTIFY_LANG").as_deref(),
        get("LC_ALL").as_deref(),
        get("LC_MESSAGES").as_deref(),
        get("LANG").as_deref(),
    )
}

/// locale 判定纯函数（不碰环境变量，可测）。
/// FNOTIFY_LANG：zh/en 大小写不敏感，空/其他值跳过；标准链首个非空 trim 值定局，
/// 前两字节 zh 前缀（大小写不敏感）→ 中文，其余 → 英文。按字节比较，避免
/// 非 ASCII 值的切界 panic。
fn decide(
    fnotify_lang: Option<&str>,
    lc_all: Option<&str>,
    lc_msg: Option<&str>,
    lang: Option<&str>,
) -> bool {
    if let Some(v) = fnotify_lang {
        let v = v.trim();
        if v.eq_ignore_ascii_case("zh") {
            return true;
        }
        if v.eq_ignore_ascii_case("en") {
            return false;
        }
    }
    for v in [lc_all, lc_msg, lang].into_iter().flatten() {
        let v = v.trim();
        if v.is_empty() {
            continue; // 空串 = 未设（locale 惯例），看下一级
        }
        let b = v.as_bytes();
        return b.len() >= 2 && b[..2].eq_ignore_ascii_case(b"zh");
    }
    false
}

/// 模板分叉：按 locale 二选一（help/usage 横幅、无占位符消息用）。
pub fn pick(en: &'static str, zh: &'static str) -> &'static str {
    if is_zh() {
        zh
    } else {
        en
    }
}

/// `pick` 的格式化版：en/zh 模板均为字面量、占位符逐一同序，按 locale
/// 选中后插入同一组实参。尾部实参原样透传给 format!（位置参数与
/// `name = value` 具名参数均可；与 suoji 的 `$arg:expr` 形态的区别仅在于
/// 此处用 tt 透传以支持具名参数）。
#[macro_export]
macro_rules! pickf {
    ($en:literal, $zh:literal $(, $($rest:tt)*)?) => {{
        if $crate::locale::is_zh() {
            format!($zh $(, $($rest)*)?)
        } else {
            format!($en $(, $($rest)*)?)
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::decide;

    #[test]
    fn fnotify_lang_explicit_override() {
        assert!(decide(Some("zh"), None, None, None));
        assert!(decide(Some(" ZH "), None, None, None));
        assert!(!decide(Some("en"), Some("zh_CN.UTF-8"), None, None));
        // 空串/其他值 → 跳过，走标准链
        assert!(decide(Some(""), Some("zh_TW"), None, None));
        assert!(decide(Some("fr"), Some("zh_TW"), None, None));
        assert!(!decide(Some(""), None, None, None));
        // 纯空白 → 跳过（同空串）
        assert!(decide(Some("   "), Some("zh_TW"), None, None));
        // 照 LANG 习惯写 FNOTIFY_LANG=zh_CN：前缀值属"其他值" → 跳过走标准链
        //（刻意：FNOTIFY_LANG 只认精确 zh/en，与 suoji 同口径，固定该行为以防"改进"时漂移）
        assert!(!decide(Some("zh_CN"), Some("en_US"), None, None));
    }

    #[test]
    fn standard_chain_priority() {
        // LC_ALL > LC_MESSAGES > LANG：首个非空值定局
        assert!(!decide(None, Some("C"), Some("zh_CN"), Some("zh_CN")));
        assert!(decide(None, Some(""), Some("zh_CN"), Some("en_US")));
        assert!(decide(None, None, None, Some("zh_CN.UTF-8")));
        assert!(!decide(None, None, Some(""), Some("en_US.UTF-8")));
        // 标准链内纯空白值同样跳过
        assert!(decide(None, Some("  "), Some("zh_CN"), None));
    }

    #[test]
    fn zh_prefix_and_case() {
        for v in ["zh_CN.UTF-8", "zh_TW", "ZH", "zh", "Zh_cn"] {
            assert!(decide(None, Some(v), None, None), "{v}");
        }
    }

    #[test]
    fn non_zh_is_english() {
        for v in ["C", "POSIX", "en_US.UTF-8", "en", "fr_FR.UTF-8", "z", "xzh"] {
            assert!(!decide(None, Some(v), None, None), "{v}");
        }
        assert!(!decide(None, None, None, None));
    }
}
