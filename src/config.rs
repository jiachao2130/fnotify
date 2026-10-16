// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! TOML 配置文件：`--config <FILE>` 加载，作为命令行解析的基线。
//!
//! 键与命令行选项一一对应（去掉前导 `-`）：`paths`（字符串数组）对应位置参数，
//! 其余 `recursive`/`globs`/`excludes`/`events`/`debounce`/`flush`/`exec`/`jobs`/`timeout`/
//! `webhook`/`quiet` 与同名选项同义、取值规则一致。未知键与类型错误一律报错（退出码 2），
//! 不在配置里静默吞掉拼错的键。
//!
//! 合并优先级：默认值 < 配置文件 < 命令行。取值型选项直接被命令行覆盖；
//! 布尔开关只增不减——命令行无法关掉配置里已开启的项。

use crate::cli::{self, Cli};
use crate::pickf;
use std::path::PathBuf;

const KNOWN_KEYS: &[&str] = &[
    "paths",
    "recursive",
    "globs",
    "excludes",
    "events",
    "debounce",
    "flush",
    "exec",
    "jobs",
    "timeout",
    "webhook",
    "oneshot",
    "until_idle",
    "quiet",
];

/// 读取并解析配置文件，返回填充好的 `Cli`（未出现的键保持默认值）。
pub fn load_config(path: &str) -> Result<Cli, String> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        pickf!(
            "cannot read config file {path}: {e}",
            "无法读取配置文件 {path}: {e}",
            path = path,
            e = e
        )
    })?;
    let table: toml::Table = text.parse().map_err(|e| {
        pickf!(
            "invalid TOML in config file {path}: {e}",
            "配置文件 {path} 的 TOML 语法错误: {e}",
            path = path,
            e = e
        )
    })?;
    build(&table)
}

fn build(t: &toml::Table) -> Result<Cli, String> {
    for key in t.keys() {
        if !KNOWN_KEYS.contains(&key.as_str()) {
            return Err(pickf!(
                "unknown key in config file: {key}",
                "配置文件含未知键: {key}",
                key = key
            ));
        }
    }
    let mut c = Cli::default();
    if let Some(paths) = str_array(t, "paths")? {
        for p in &paths {
            if p.is_empty() {
                return Err(pickf!(
                    "config key `paths` contains an empty path",
                    "配置键 `paths` 含空路径"
                ));
            }
        }
        c.paths = paths.into_iter().map(PathBuf::from).collect();
    }
    if let Some(b) = boolean(t, "recursive")? {
        c.recursive = b;
    }
    if let Some(globs) = str_array(t, "globs")? {
        for g in &globs {
            if g.is_empty() {
                return Err(pickf!(
                    "config key `globs` contains an empty pattern",
                    "配置键 `globs` 含空模式"
                ));
            }
        }
        c.globs = globs;
    }
    if let Some(excludes) = str_array(t, "excludes")? {
        for g in &excludes {
            if g.is_empty() {
                return Err(pickf!(
                    "config key `excludes` contains an empty pattern",
                    "配置键 `excludes` 含空模式"
                ));
            }
        }
        c.excludes = excludes;
    }
    if let Some(s) = string(t, "events")? {
        c.event_mask = cli::parse_events(&s)?;
    }
    if let Some(n) = integer(t, "debounce")? {
        if n < 0 {
            return Err(pickf!(
                "config key `debounce` must be a non-negative integer (ms)",
                "配置键 `debounce` 应为非负整数（毫秒）"
            ));
        }
        c.debounce_ms = (n as u64).max(10); // 与命令行一致：小于 10 按 10 计
    }
    if let Some(s) = string(t, "flush")? {
        c.flush_immediate = cli::parse_flush(&s)?;
    }
    if let Some(s) = string(t, "exec")? {
        if s.is_empty() {
            return Err(pickf!(
                "config key `exec` must not be empty",
                "配置键 `exec` 不能为空"
            ));
        }
        c.exec = Some(s);
    }
    if let Some(n) = integer(t, "jobs")? {
        if n < 0 || n > u32::MAX as i64 {
            return Err(pickf!(
                "config key `jobs` must be a non-negative integer (at most {max})",
                "配置键 `jobs` 应为非负整数（不大于 {max}）",
                max = u32::MAX
            ));
        }
        c.jobs = (n as u32).max(1); // 0 按 1 计（串行），与命令行一致
    }
    if let Some(n) = integer(t, "timeout")? {
        if n < 0 {
            return Err(pickf!(
                "config key `timeout` must be a non-negative integer (seconds)",
                "配置键 `timeout` 应为非负整数（秒）"
            ));
        }
        c.timeout_sec = n as u64;
    }
    if let Some(s) = string(t, "webhook")? {
        if s.is_empty() {
            return Err(pickf!(
                "config key `webhook` must not be empty",
                "配置键 `webhook` 不能为空"
            ));
        }
        c.webhook = Some(s);
    }
    if let Some(b) = boolean(t, "oneshot")? {
        c.oneshot = b;
    }
    if let Some(n) = integer(t, "until_idle")? {
        if n < 1 {
            return Err(pickf!(
                "config key `until_idle` must be a positive integer (seconds)",
                "配置键 `until_idle` 应为正整数（秒）"
            ));
        }
        c.until_idle = Some(n as u64);
    }
    if let Some(b) = boolean(t, "quiet")? {
        c.quiet = b;
    }
    Ok(c)
}

fn type_err(key: &str, expect: &str) -> String {
    pickf!(
        "config key `{key}` has wrong type (expect {expect})",
        "配置键 `{key}` 类型错误（应为 {expect}）",
        key = key,
        expect = expect
    )
}

fn boolean(t: &toml::Table, key: &str) -> Result<Option<bool>, String> {
    match t.get(key) {
        None => Ok(None),
        Some(toml::Value::Boolean(b)) => Ok(Some(*b)),
        Some(_) => Err(type_err(key, "bool")),
    }
}

fn string(t: &toml::Table, key: &str) -> Result<Option<String>, String> {
    match t.get(key) {
        None => Ok(None),
        Some(toml::Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(type_err(key, "string")),
    }
}

fn str_array(t: &toml::Table, key: &str) -> Result<Option<Vec<String>>, String> {
    match t.get(key) {
        None => Ok(None),
        Some(toml::Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    toml::Value::String(s) => out.push(s.clone()),
                    _ => return Err(type_err(key, "array of strings")),
                }
            }
            Ok(Some(out))
        }
        Some(_) => Err(type_err(key, "array of strings")),
    }
}

fn integer(t: &toml::Table, key: &str) -> Result<Option<i64>, String> {
    match t.get(key) {
        None => Ok(None),
        Some(toml::Value::Integer(n)) => Ok(Some(*n)),
        Some(_) => Err(type_err(key, "integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{BIT_ACCESS, BIT_CREATE, BIT_MODIFY, BIT_OTHER, BIT_REMOVE, BIT_RENAME};
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// 把 TOML 文本落盘再加载，走与 `--config` 完全相同的读文件路径。
    fn load(text: &str) -> Result<Cli, String> {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("fnotify-cfg-test-{}-{n}.toml", std::process::id()));
        std::fs::write(&path, text).expect("写临时配置");
        let r = load_config(path.to_str().unwrap());
        let _ = std::fs::remove_file(&path);
        r
    }

    #[test]
    fn full_config_maps_every_key() {
        let c = load(
            r#"
paths = ["/var/log", "/etc"]
recursive = true
globs = ["*.log", "*.conf"]
excludes = ["*.tmp"]
events = "access,modify"
debounce = 200
flush = "immediate"
exec = "logger %e %p"
jobs = 4
timeout = 30
webhook = "http://127.0.0.1:9/hook"
oneshot = true
until_idle = 5
quiet = true
"#,
        )
        .expect("合法配置应加载成功");
        assert_eq!(
            c.paths,
            vec![PathBuf::from("/var/log"), PathBuf::from("/etc")]
        );
        assert!(c.recursive);
        assert_eq!(c.globs, vec!["*.log", "*.conf"]);
        assert_eq!(c.excludes, vec!["*.tmp"]);
        assert_eq!(c.event_mask, BIT_ACCESS | BIT_MODIFY);
        assert_eq!(c.debounce_ms, 200);
        assert!(c.flush_immediate);
        assert_eq!(c.exec.as_deref(), Some("logger %e %p"));
        assert_eq!(c.jobs, 4);
        assert_eq!(c.timeout_sec, 30);
        assert_eq!(c.webhook.as_deref(), Some("http://127.0.0.1:9/hook"));
        assert!(c.oneshot);
        assert_eq!(c.until_idle, Some(5));
        assert!(c.quiet);
    }

    #[test]
    fn sparse_config_keeps_defaults() {
        let c = load("recursive = true\n").expect("加载失败");
        assert!(c.recursive);
        assert_eq!(c.paths, Cli::default().paths);
        assert_eq!(c.event_mask, Cli::default().event_mask);
        assert_eq!(c.debounce_ms, 50);
        assert_eq!(c.jobs, 1);
        assert!(!c.quiet && !c.flush_immediate);
        assert!(c.exec.is_none() && c.webhook.is_none());
    }

    #[test]
    fn unknown_key_rejected() {
        assert!(load("recursiv = true\n").is_err());
    }

    #[test]
    fn wrong_type_rejected() {
        for bad in [
            "debounce = \"abc\"\n",
            "debounce = 1.5\n",
            "recursive = \"yes\"\n",
            "paths = \"/var/log\"\n",
            "paths = [1, 2]\n",
            "globs = [\"*.log\", 3]\n",
            "excludes = [\"*.tmp\", 3]\n",
        ] {
            assert!(load(bad).is_err(), "应当报错: {bad}");
        }
    }

    #[test]
    fn numeric_clamps_match_cli() {
        let c = load("debounce = 5\njobs = 0\n").expect("加载失败");
        assert_eq!(c.debounce_ms, 10); // 与命令行一致：小于 10 按 10 计
        assert_eq!(c.jobs, 1); // 0 按 1 计（串行）
        assert!(load("debounce = -1\n").is_err());
        assert!(load("jobs = -2\n").is_err());
        assert!(load("timeout = -1\n").is_err());
        // 超 u32 上界报错，与命令行 parse_jobs 的口径一致（不静默截断）
        assert!(load("jobs = 5000000000\n").is_err());
    }

    #[test]
    fn event_and_flush_rules_reused_from_cli() {
        // events/flush 复用 cli 的解析：取值集合与报错口径与命令行完全一致。
        let c = load("events = \"all\"\nflush = \"immediate\"\n").expect("加载失败");
        assert_eq!(
            c.event_mask,
            BIT_CREATE
                | BIT_MODIFY
                | BIT_REMOVE
                | BIT_ACCESS
                | BIT_RENAME
                | BIT_OTHER
                | crate::engine::BIT_METADATA
                | crate::engine::BIT_CLOSE_WRITE
        );
        assert!(c.flush_immediate);
        assert!(load("events = \"creat\"\n").is_err());
        assert!(load("flush = \"now\"\n").is_err());
    }

    #[test]
    fn oneshot_and_until_idle_config_keys() {
        // 新键映射与取值规则；until_idle <1 报错（不启用 = 省略该键）。
        let c = load("oneshot = true\nuntil_idle = 5\n").expect("合法配置应加载成功");
        assert!(c.oneshot);
        assert_eq!(c.until_idle, Some(5));
        let c = load("oneshot = false\n").expect("合法配置应加载成功");
        assert!(!c.oneshot);
        assert_eq!(c.until_idle, None);
        assert!(load("until_idle = 0\n").is_err());
        assert!(load("until_idle = -1\n").is_err());
        assert!(load("until_idle = \"5\"\n").is_err());
        assert!(load("oneshot = \"yes\"\n").is_err());
    }

    #[test]
    fn empty_strings_rejected() {
        for bad in [
            "paths = [\"\"]\n",
            "globs = [\"\"]\n",
            "excludes = [\"\"]\n",
            "exec = \"\"\n",
            "webhook = \"\"\n",
        ] {
            assert!(load(bad).is_err(), "应当报错: {bad}");
        }
    }

    #[test]
    fn missing_file_is_error() {
        assert!(load_config("/nonexistent/fnotify-config.toml").is_err());
    }
}
