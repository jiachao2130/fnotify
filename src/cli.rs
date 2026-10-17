// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! 手写参数解析（不依赖 clap）。
//!
//! ```text
//! fnotify [OPTIONS] [PATH]...        # PATH 默认 "."，可多个
//! ```

use crate::engine;
use crate::locale::pick;
use crate::pickf;
use std::path::PathBuf;

pub const VERSION: &str = concat!("fnotify ", env!("CARGO_PKG_VERSION"));

/// usage 行（除前导词外中英文逐字一致；成对常量经 locale::pick 选择）。
/// HELP_* 的首行与之一致（由本文件单测保证）。
pub const USAGE_EN: &str = "usage: fnotify [OPTIONS] [PATH]...";
pub const USAGE_ZH: &str = "用法: fnotify [OPTIONS] [PATH]...";

/// -h/--help 全文（双语成对；-word token 集合相等由本文件单测保证）。
pub const HELP_EN: &str = "usage: fnotify [OPTIONS] [PATH]...

fnotify — file event router: watch -> debounce -> glob filter -> JSONL / exec

  PATH                    watch directory, repeatable, default \".\"
                          (canonicalized to an absolute path at startup;
                          exit code 2 if it does not exist)

options:
  -r, --recursive         watch subdirectories recursively (default: the directory itself only)
  -g, --glob <PATTERN>    filter by file name (last path component); repeatable,
                          any match passes; supports * ? [abc] [a-z] [!abc];
                          * does not cross /
      --exclude <PATTERN> reverse filter by file name; repeatable, a match
                          suppresses the event (wins over -g); exclude only,
                          no -g = everything but these
  -e, --events <LIST>     comma-separated: create,modify,remove,access,rename,metadata,close_write,other,all
                          default create,modify,rename,remove
      --debounce <MS>     debounce window in milliseconds, default 50, minimum 10
      --flush <MODE>      debounce (default) or immediate: immediate delivers the
                          first event of each (path,event) at once and suppresses
                          repeats for one debounce window
  -x, --exec <CMD>        command template run via sh -c per event;
                          %p=event path (POSIX single-quoted) %e=event name
      --jobs <N>          max concurrent exec processes, default 1 (serial)
      --timeout <SEC>     per-exec timeout in seconds, default 0 (no limit)
      --webhook <URL>     POST each batch as a JSON array to this http:// URL
      --oneshot           exit gracefully once the first batch that passes the
                          filters is delivered
      --until-idle <SEC>  exit gracefully after SEC seconds without a delivered
                          batch; ongoing events keep it running
      --config <FILE>     load options from a TOML file; command-line options
                          override it (boolean flags are additive)
  -q, --quiet             disable JSONL stdout output (often combined with -x)
  -h, --help              show this help
  -V, --version           show version

output (default): one JSONL record per line to stdout:
  {\"type\":\"event\",\"ts\":<RFC3339 UTC>,\"event\":\"<name>\",\"path\":\"</abs/path>\",\"dir\":<bool>}
  with -x (and without -q), exec_start/exec_end lifecycle records join the stream

exit status: 0 ok (including signal shutdown and watch-root removal);
  1 runtime error; 2 usage error

examples:
  fnotify -r /var/log                          # recursive watch, default events, JSONL output
  fnotify -g '*.log' -g '*.conf' /etc          # only care about .log and .conf
  fnotify -r -q -x 'logger -t fnotify -- %e %p' .  # silent JSONL, events to syslog
  fnotify -e access,modify --debounce 200 src  # access/modify, 200ms debounce
  fnotify --config fnotify.toml -g '*.log' .      # config as base, CLI glob/path win
  fnotify --oneshot -e close_write -g '*.zip' ./upload  # wait for one upload, then exit
";

pub const HELP_ZH: &str = "用法: fnotify [OPTIONS] [PATH]...

fnotify — 文件事件路由器：监听 → 去抖 → glob 过滤 → JSONL / 执行命令

  PATH                    监听目录，可多个，默认 \".\"
                          （启动时规范化为绝对路径；不存在则退出码 2）

选项:
  -r, --recursive         递归监听子目录（默认仅监听目录本身）
  -g, --glob <PATTERN>    按文件名（路径最后一段）过滤；可重复，任一命中即通过
                          支持 * ? [abc] [a-z] [!abc]；* 不跨 /
      --exclude <PATTERN> 按文件名反向过滤；可重复，命中即不上报（优先于 -g）；
                          只给 --exclude 等于“除了这些全都要”
  -e, --events <LIST>     逗号分隔: create,modify,remove,access,rename,metadata,close_write,other,all
                          默认 create,modify,rename,remove
      --debounce <MS>     去抖窗口毫秒数，默认 50，小于 10 时按 10 计
      --flush <MODE>      debounce（默认）或 immediate：每个（路径,事件名）首事件
                          立即交付，一个窗口内抑制重复
  -x, --exec <CMD>        每个事件经 sh -c 执行的命令模板；
                          %p=事件路径（POSIX 单引号包裹） %e=事件名
      --jobs <N>          exec 最大并发进程数，默认 1（串行）
      --timeout <SEC>     单个 exec 超时秒数，默认 0（不限制）
      --webhook <URL>     每个批次以 JSON 数组 POST 到该 http:// 地址
      --oneshot           首个通过过滤的批次交付后优雅退出
      --until-idle <SEC>  批次交付静默满 SEC 秒后优雅退出；持续事件则一直运行
      --config <FILE>     从 TOML 配置文件加载选项；命令行优先（布尔开关只增不减）
  -q, --quiet             关闭 JSONL stdout 输出（常与 -x 连用）
  -h, --help              显示本帮助
  -V, --version           显示版本

输出（默认）: 每行一条 JSONL 记录到 stdout:
  {\"type\":\"event\",\"ts\":<RFC3339 UTC>,\"event\":\"<名>\",\"path\":\"</abs/path>\",\"dir\":<bool>}
  配置 -x 且未 -q 时，exec_start/exec_end 生命周期记录进入同一输出流

退出码: 0 正常（含信号停机与监听目录删除）；1 运行错误；2 用法错误

示例:
  fnotify -r /var/log                          # 递归监听，默认事件，JSONL 输出
  fnotify -g '*.log' -g '*.conf' /etc          # 只关心 .log 与 .conf
  fnotify -r -q -x 'logger -t fnotify -- %e %p' .  # 静默 JSONL，事件转 syslog
  fnotify -e access,modify --debounce 200 src  # 访问/修改，200ms 去抖
  fnotify --config fnotify.toml -g '*.log' .      # 配置作基线，命令行的 glob/路径优先
  fnotify --oneshot -e close_write -g '*.zip' ./upload  # 等到一个上传完成即退出
";

pub fn help() -> &'static str {
    pick(HELP_EN, HELP_ZH)
}

pub fn usage() -> &'static str {
    pick(USAGE_EN, USAGE_ZH)
}

/// 解析结果：Run 继续执行；Help / Version 打印后即退出。
#[derive(Debug)]
pub enum Action {
    Run(Cli),
    Help,
    Version,
}

#[derive(Debug)]
pub struct Cli {
    pub paths: Vec<PathBuf>,
    pub recursive: bool,
    pub globs: Vec<String>,
    /// `--exclude` 反向过滤模式；与 globs 同为替换语义（首个 CLI --exclude 起替换配置值）。
    pub excludes: Vec<String>,
    pub event_mask: u32,
    pub debounce_ms: u64,
    pub flush_immediate: bool,
    pub exec: Option<String>,
    pub jobs: u32,
    pub timeout_sec: u64,
    pub webhook: Option<String>,
    /// `--oneshot`：首个通过过滤的批次交付后优雅退出。
    pub oneshot: bool,
    /// `--until-idle SEC`：批次交付静默满 SEC 秒后优雅退出；None = 不启用。
    pub until_idle: Option<u64>,
    pub quiet: bool,
}

impl Default for Cli {
    fn default() -> Self {
        Cli {
            paths: Vec::new(),
            recursive: false,
            globs: Vec::new(),
            excludes: Vec::new(),
            event_mask: engine::DEFAULT_EVENTS,
            debounce_ms: 50,
            flush_immediate: false,
            exec: None,
            jobs: 1,
            timeout_sec: 0,
            webhook: None,
            oneshot: false,
            until_idle: None,
            quiet: false,
        }
    }
}

/// 解析 `argv[1..]`。参数错误返回 `Err(错误信息)`，由 main 输出到 stderr 并 exit 2。
///
/// 先预扫描 `--config <FILE>`：配置文件加载结果作为基线，命令行选项在其上叠加，
/// 优先级 默认值 < 配置 < 命令行。取值型选项直接被命令行覆盖；布尔开关只增不减
/// （命令行无法关掉配置里已开启的项）。
pub fn parse(args: &[String]) -> Result<Action, String> {
    // 预扫描 --config（同时把该选项及其取值从主循环参数里剔除）。
    let mut base = Cli::default();
    let mut rest: Vec<String> = Vec::with_capacity(args.len());
    let mut i = 0usize;
    let mut seen_config = false;
    while i < args.len() {
        let arg = args[i].as_str();
        // 终结符：其后一切（含字面 --config=...）按位置参数处理，预扫描到此
        // 为止——与主循环「首个 -- 起全按路径」的语义保持一致（验收复核抓到的
        // 口子：fnotify /data -- --config=evil.toml 曾被预扫描加载并剥离）
        if arg == "--" {
            rest.extend_from_slice(&args[i..]);
            break;
        }
        if arg == "--config" || arg.starts_with("--config=") {
            let inline = arg
                .split_once('=')
                .filter(|(f, _)| *f == "--config")
                .map(|(_, v)| v.to_string());
            let path = take_value(args, &mut i, "--config", inline)?;
            if seen_config {
                return Err(pickf!("duplicate --config option", "--config 重复指定"));
            }
            seen_config = true;
            base = crate::config::load_config(&path)?;
        } else {
            rest.push(args[i].clone());
        }
        i += 1;
    }

    let mut c = base;
    // 命令行显式给出 -g / --exclude / 位置参数时，替换配置里的对应值；未给出则沿用配置。
    let mut cli_globs = false;
    let mut cli_excludes = false;
    let mut cli_paths = false;
    // `--` 终结符之后的参数一律按位置参数处理（监听以 - 开头的路径）；
    // 首个 -- 起作用的语义，第二个 -- 本身算作路径。
    let mut positional_only = false;
    let mut i = 0usize;
    while i < rest.len() {
        let arg = rest[i].as_str();
        if arg == "--" && !positional_only {
            // 终结符：其后一律按位置参数（含字面 -x/--foo，不做任何选项匹配）
            positional_only = true;
            i += 1;
            continue;
        }
        if positional_only {
            if !cli_paths {
                // 与常规位置参数同规则：命令行首个位置参数起替换配置里的路径
                c.paths.clear();
                cli_paths = true;
            }
            c.paths.push(PathBuf::from(arg));
            i += 1;
            continue;
        }
        // 短 flag 粘连：-rq 等价 -r -q。仅限布尔短 flag（r/q/h/V）；
        // 含取值型短 flag（e/g/x）或未知字母的，落入常规匹配报未知选项。
        if arg.starts_with('-') && !arg.starts_with("--") && arg.chars().nth(1).is_some() {
            let letters: Vec<char> = arg.chars().skip(1).collect();
            if letters.len() > 1 && letters.iter().all(|c| matches!(c, 'r' | 'q' | 'h' | 'V')) {
                for ch in letters {
                    match ch {
                        'r' => c.recursive = true,
                        'q' => c.quiet = true,
                        'h' => return Ok(Action::Help),
                        'V' => return Ok(Action::Version),
                        _ => unreachable!(),
                    }
                }
                i += 1;
                continue;
            }
        }
        // 长选项支持 --flag=value 形式
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (f, Some(v.to_string())),
            _ => (arg, None),
        };
        match flag {
            "-h" | "--help" => return Ok(Action::Help),
            "-V" | "--version" => return Ok(Action::Version),
            "-r" | "--recursive" => c.recursive = true,
            "-q" | "--quiet" => c.quiet = true,
            "-g" | "--glob" => {
                if !cli_globs {
                    // 命令行第一个 -g 起用：清空配置里的模式再累积
                    c.globs.clear();
                    cli_globs = true;
                }
                c.globs.push(take_value(&rest, &mut i, arg, inline)?)
            }
            "--exclude" => {
                if !cli_excludes {
                    // 与 -g 同规则：首个 CLI --exclude 起替换配置里的 excludes
                    c.excludes.clear();
                    cli_excludes = true;
                }
                c.excludes.push(take_value(&rest, &mut i, arg, inline)?)
            }
            "-e" | "--events" => {
                c.event_mask = parse_events(&take_value(&rest, &mut i, arg, inline)?)?
            }
            "--debounce" => {
                c.debounce_ms = parse_debounce(&take_value(&rest, &mut i, arg, inline)?)?
            }
            "--flush" => c.flush_immediate = parse_flush(&take_value(&rest, &mut i, arg, inline)?)?,
            "-x" | "--exec" => c.exec = Some(take_value(&rest, &mut i, arg, inline)?),
            "--jobs" => c.jobs = parse_jobs(&take_value(&rest, &mut i, arg, inline)?)?,
            "--timeout" => c.timeout_sec = parse_timeout(&take_value(&rest, &mut i, arg, inline)?)?,
            "--webhook" => c.webhook = Some(take_value(&rest, &mut i, arg, inline)?),
            "--oneshot" => c.oneshot = true,
            "--until-idle" => {
                c.until_idle = Some(parse_until_idle(&take_value(&rest, &mut i, arg, inline)?)?)
            }
            _ if flag.starts_with('-') && flag != "-" => {
                return Err(pickf!("unknown option: {arg}", "未知选项: {arg}"));
            }
            _ => {
                if !cli_paths {
                    // 命令行第一个位置参数起用：替换配置里的监听路径
                    c.paths.clear();
                    cli_paths = true;
                }
                c.paths.push(PathBuf::from(arg))
            }
        }
        i += 1;
    }
    if c.paths.is_empty() {
        c.paths.push(PathBuf::from("."));
    }
    Ok(Action::Run(c))
}

/// 取选项值：优先 `--flag=value` 内联值，否则消费下一个 argv；缺失或空串即报错。
/// 空串在这里拦下（如空 `-g` 会静默过滤一切、空 `-x` 每事件跑一次空命令）。
fn take_value(
    args: &[String],
    i: &mut usize,
    flag: &str,
    inline: Option<String>,
) -> Result<String, String> {
    let v = if let Some(v) = inline {
        v
    } else {
        let next = args
            .get(*i + 1)
            .ok_or_else(|| pickf!("{flag} requires an argument", "{flag} 需要一个参数"))?;
        if next.starts_with('-') && next != "-" {
            return Err(pickf!(
                "{flag} requires an argument, got {next}",
                "{flag} 需要一个参数，却得到 {next}"
            ));
        }
        *i += 1; // 消费值
        next.clone()
    };
    if v.is_empty() {
        return Err(pickf!(
            "{flag} requires a non-empty value",
            "{flag} 的参数不能为空"
        ));
    }
    Ok(v)
}

/// `-e` 合法事件名表（唯一权威清单）。help 横幅（HELP_EN/HELP_ZH）与
/// `parse_events` 错误消息都从这里取列表，新增事件名只改这一处；
/// `event_value_list_consistent_everywhere` 单测再兜底一遍四处一致
/// （防 metadata 轮那种"解析表改了、消息列表漏了"的遗漏重演）。
pub(crate) const EVENT_CHOICES: &str =
    "create,modify,remove,access,rename,metadata,close_write,other,all";

pub(crate) fn parse_events(list: &str) -> Result<u32, String> {
    let mut mask = 0u32;
    for token in list.split(',') {
        let t = token.trim();
        if t.is_empty() {
            return Err(pickf!("invalid event list: {list}", "非法事件列表: {list}"));
        }
        match engine::event_bit_for_token(t) {
            Some(bit) => mask |= bit,
            None => {
                return Err(pickf!(
                    "invalid event name: {t} (choose from {choices})",
                    "非法事件名: {t}（可选 {choices}）",
                    t = t,
                    choices = EVENT_CHOICES
                ))
            }
        }
    }
    Ok(mask)
}

fn parse_debounce(v: &str) -> Result<u64, String> {
    let ms: u64 = v.parse().map_err(|_| {
        pickf!(
            "invalid --debounce value: {v} (non-negative integer ms expected)",
            "非法 --debounce 值: {v}（需要非负整数毫秒）"
        )
    })?;
    Ok(ms.max(10)) // <10 按 10 处理
}

/// --flush 取值：debounce（默认，trailing-edge 聚合）或 immediate
/// （leading-edge：每个（路径,事件名）首事件立即交付，窗口内抑制重复）。
pub(crate) fn parse_flush(v: &str) -> Result<bool, String> {
    match v {
        "debounce" => Ok(false),
        "immediate" => Ok(true),
        _ => Err(pickf!(
            "invalid flush mode: {v} (debounce or immediate)",
            "非法 flush 取值: {v}（debounce 或 immediate）"
        )),
    }
}

/// --jobs 取值：exec 并发上限；非法值报错，<1 按 1 计（串行）。
fn parse_jobs(v: &str) -> Result<u32, String> {
    match v.parse::<u32>() {
        Ok(0) => Ok(1),
        Ok(n) => Ok(n),
        Err(_) => Err(pickf!(
            "invalid --jobs value: {v} (positive integer)",
            "非法 --jobs 值: {v}（正整数）"
        )),
    }
}

/// --timeout 取值：单个 exec 的超时秒数；0 = 不限制。
fn parse_timeout(v: &str) -> Result<u64, String> {
    v.parse::<u64>().map_err(|_| {
        pickf!(
            "invalid --timeout value: {v} (non-negative integer)",
            "非法 --timeout 值: {v}（非负整数）"
        )
    })
}

/// until-idle 零值的报错文案：CLI `--until-idle 0` 与库 API `Config::until_idle(0)`
/// 共用一份（config 键另有指名键名的语境化文案，见 config.rs）。
pub(crate) fn until_idle_zero_msg(v: &str) -> String {
    pickf!(
        "invalid --until-idle value: {v} (positive integer seconds; omit the flag to disable)",
        "非法 --until-idle 值: {v}（正整数秒；不启用请省略该选项）"
    )
}

/// --until-idle 取值：静默秒数。0 与负数报错——"不启用"请省略该选项，
/// 0 秒在语义上等于"交付后立即退出"，没有真实使用场景，报错比静默截断更诚实。
fn parse_until_idle(v: &str) -> Result<u64, String> {
    match v.parse::<u64>() {
        Ok(0) => Err(until_idle_zero_msg(v)),
        Ok(n) => Ok(n),
        Err(_) => Err(pickf!(
            "invalid --until-idle value: {v} (positive integer seconds)",
            "非法 --until-idle 值: {v}（正整数秒）"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{BIT_ACCESS, BIT_CREATE, BIT_MODIFY, BIT_OTHER, BIT_REMOVE, BIT_RENAME};

    fn run(args: &[&str]) -> Cli {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        match parse(&args) {
            Ok(Action::Run(c)) => c,
            other => panic!("期望 Run，得到 {other:?}"),
        }
    }

    #[test]
    fn defaults() {
        let c = run(&[]);
        assert_eq!(c.paths, vec![PathBuf::from(".")]);
        assert!(!c.recursive);
        assert!(c.globs.is_empty());
        assert!(c.excludes.is_empty());
        // 默认事件集在此显式固定（与 engine::DEFAULT_EVENTS 同步维护）。
        assert_eq!(
            c.event_mask,
            BIT_CREATE | BIT_MODIFY | BIT_RENAME | BIT_REMOVE
        );
        assert_eq!(c.debounce_ms, 50);
        assert!(!c.flush_immediate);
        assert_eq!(c.jobs, 1);
        assert_eq!(c.timeout_sec, 0);
        assert!(c.webhook.is_none());
        assert!(c.exec.is_none());
        assert!(!c.quiet);
    }

    #[test]
    fn double_dash_terminator_treats_rest_as_paths() {
        // `--` 之后以 - 开头的参数按路径处理（监听名为 -foo 的目录）
        let c = run(&["-r", "--", "-weird-dir", "-x"]);
        assert!(c.recursive);
        assert_eq!(
            c.paths,
            vec![PathBuf::from("-weird-dir"), PathBuf::from("-x")],
            "-- 之后不得再当选项解析"
        );
        // 第二个 -- 本身算路径；-- 之前的选项照常解析
        let c = run(&["-q", "--", "--", "-p"]);
        assert!(c.quiet);
        assert_eq!(c.paths, vec![PathBuf::from("--"), PathBuf::from("-p")]);
        // 不带位置参数的裸 --：沿用默认路径
        let c = run(&["--"]);
        assert_eq!(c.paths, vec![PathBuf::from(".")]);
    }

    #[test]
    fn flags_and_values() {
        let c = run(&[
            "-r",
            "-g",
            "*.log",
            "--glob=*.conf",
            "-e",
            "rename,other",
            "--debounce=5",
            "--flush",
            "immediate",
            "--jobs",
            "4",
            "--timeout=30",
            "-x",
            "make",
            "-q",
            "/tmp",
        ]);
        assert!(c.recursive && c.quiet);
        assert_eq!(c.globs, vec!["*.log", "*.conf"]);
        assert_eq!(c.event_mask, BIT_RENAME | BIT_OTHER);
        assert_eq!(c.debounce_ms, 10); // 5 按 10 处理
        assert!(c.flush_immediate);
        assert_eq!(c.jobs, 4);
        assert_eq!(c.timeout_sec, 30);
        assert_eq!(c.exec.as_deref(), Some("make"));
        assert_eq!(c.paths, vec![PathBuf::from("/tmp")]);
    }

    #[test]
    fn exclude_parsing_and_replacement() {
        // 可重复累积；与 -g 搭配各自独立。
        let c = run(&[
            "--exclude",
            "*.tmp",
            "--exclude=*.swp",
            "-g",
            "*.log",
            "/tmp",
        ]);
        assert_eq!(c.excludes, vec!["*.tmp", "*.swp"]);
        assert_eq!(c.globs, vec!["*.log"]);
        // CLI --exclude 替换配置 excludes（首个起替换，与 -g 同规则）。
        let r = run_with_config("excludes = [\"*.bak\"]\n", &["--exclude", "*.tmp"])
            .expect("应加载成功");
        let Action::Run(c) = r else {
            panic!("期望 Run")
        };
        assert_eq!(c.excludes, vec!["*.tmp"]);
        // CLI 未给 --exclude 时配置值保留。
        let r = run_with_config("excludes = [\"*.bak\"]\n", &[]).expect("应加载成功");
        let Action::Run(c) = r else {
            panic!("期望 Run")
        };
        assert_eq!(c.excludes, vec!["*.bak"]);
    }

    #[test]
    fn oneshot_and_until_idle_parsing() {
        // 默认不启用。
        let c = run(&["/tmp"]);
        assert!(!c.oneshot);
        assert_eq!(c.until_idle, None);
        // --oneshot 布尔开关；--until-idle 取值型、可重复（后者覆盖前者）。
        let c = run(&["--oneshot", "--until-idle", "5", "/tmp"]);
        assert!(c.oneshot);
        assert_eq!(c.until_idle, Some(5));
        // 取值型选项直接被命令行覆盖（与配置合并的口径一致）。
        let c = run(&["--until-idle=3", "--until-idle", "9", "/tmp"]);
        assert_eq!(c.until_idle, Some(9));
    }

    #[test]
    fn jobs_below_one_is_serial() {
        let c = run(&["--jobs", "0", "/tmp"]);
        assert_eq!(c.jobs, 1);
    }

    #[test]
    fn event_value_list_consistent_everywhere() {
        // help 横幅（中英两处）与错误消息（经 EVENT_CHOICES 派生）中的
        // -e 合法值表必须与权威清单逐 token 一致。
        let canonical: Vec<&str> = EVENT_CHOICES.split(',').collect();
        let en_line = HELP_EN
            .split("comma-separated: ")
            .nth(1)
            .and_then(|s| s.lines().next())
            .expect("HELP_EN 应含 -e 列表行");
        let zh_line = HELP_ZH
            .split("逗号分隔: ")
            .nth(1)
            .and_then(|s| s.lines().next())
            .expect("HELP_ZH 应含 -e 列表行");
        assert_eq!(en_line, EVENT_CHOICES, "HELP_EN 列表漂移");
        assert_eq!(zh_line, EVENT_CHOICES, "HELP_ZH 列表漂移");
        // 清单每个 token 都必须被解析表接受（防"消息列了、引擎不认"）。
        for tok in &canonical {
            assert!(parse_events(tok).is_ok(), "解析表拒收清单内的 {tok}");
        }
        // 清单外的名字必须被拒（反向护栏）。
        assert!(parse_events("bogus").is_err());
        assert_eq!(canonical.len(), 9);
    }

    #[test]
    fn glued_short_flags() {
        // -rq 等价 -r -q；h/V 在粘连中同样立即生效。
        let c = run(&["-rq", "/tmp"]);
        assert!(c.recursive && c.quiet);
        let args: Vec<String> = vec!["-rh".into()];
        assert!(matches!(parse(&args), Ok(Action::Help)));
    }

    #[test]
    fn glued_short_flags_reject_unknown() {
        // 含未知字母或取值型短 flag 的粘连不算 glued，走常规未知选项报错。
        for bad in ["-rz", "-rx", "-ez"] {
            let args: Vec<String> = vec![bad.into(), "/tmp".into()];
            assert!(parse(&args).is_err(), "应当报错: {bad}");
        }
    }

    #[test]
    fn events_all() {
        let c = run(&["-e", "all"]);
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
    }

    #[test]
    fn events_close_write_is_opt_in() {
        // close_write（IN_CLOSE_WRITE）独立成类且不在默认集：掩码层 opt-in，
        // 不订阅则内核不产出、引擎连读都不读。
        let c = run(&["-e", "close_write"]);
        assert_eq!(c.event_mask, crate::engine::BIT_CLOSE_WRITE);
        assert_eq!(run(&[]).event_mask, crate::engine::DEFAULT_EVENTS);
    }

    #[test]
    fn events_metadata_is_distinct() {
        // metadata（IN_ATTRIB）独立成类：可与 modify 分别订阅，互不替代。
        let c = run(&["-e", "metadata"]);
        assert_eq!(c.event_mask, crate::engine::BIT_METADATA);
    }

    #[test]
    fn errors_exit_path() {
        for bad in [
            vec!["--nope"],
            vec!["-e", "creat"],
            vec!["-e", ""],
            vec!["--debounce", "abc"],
            vec!["--flush", "now"],
            vec!["--flush="],
            vec!["--jobs", "x"],
            vec!["--jobs="],
            vec!["--timeout", "-1"],
            vec!["--timeout="],
            vec!["--webhook="],
            vec!["--exclude"],
            vec!["--exclude="],
            vec!["--until-idle"],
            vec!["--until-idle="],
            vec!["--until-idle", "0"],
            vec!["--until-idle", "abc"],
            vec!["-g"],
            vec!["--events"],
            vec!["-g", ""],  // 空 glob 会静默过滤一切
            vec!["--glob="], // 内联空值同理
            vec!["-x", ""],  // 空 exec 每事件跑一次空命令
            vec!["--exec="],
        ] {
            let args: Vec<String> = bad.iter().map(|s| s.to_string()).collect();
            assert!(parse(&args).is_err(), "应当报错: {bad:?}");
        }
    }

    #[test]
    fn webhook_value_parsed() {
        let c = run(&["--webhook", "http://127.0.0.1:9/hook", "/tmp"]);
        assert_eq!(c.webhook.as_deref(), Some("http://127.0.0.1:9/hook"));
    }

    static CFG_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// 把 TOML 文本落盘再让 parse() 走 --config 加载，验证合并语义。
    /// 文件名带序号：测试并行跑，同 pid 下不能共用临时文件。
    fn run_with_config(toml_text: &str, extra: &[&str]) -> Result<Action, String> {
        let n = CFG_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("fnotify-cli-cfg-{}-{n}.toml", std::process::id()));
        std::fs::write(&path, toml_text).expect("写临时配置");
        let mut args: Vec<String> = vec!["--config".into(), path.to_string_lossy().into_owned()];
        args.extend(extra.iter().map(|s| s.to_string()));
        let r = parse(&args);
        let _ = std::fs::remove_file(&path);
        r
    }

    #[test]
    fn config_file_is_the_baseline() {
        let r = run_with_config(
            "recursive = true\nglobs = [\"*.log\"]\ndebounce = 200\n",
            &[],
        )
        .expect("应加载成功");
        let Action::Run(c) = r else {
            panic!("期望 Run")
        };
        assert!(c.recursive); // 配置开启的布尔项，无命令行时生效
        assert_eq!(c.globs, vec!["*.log"]);
        assert_eq!(c.debounce_ms, 200);
        assert_eq!(c.paths, vec![PathBuf::from(".")]); // 配置没给路径，回落默认
    }

    #[test]
    fn cli_overrides_config_values() {
        let r = run_with_config(
            "debounce = 200\nglobs = [\"*.log\"]\npaths = [\"/etc\"]\nevents = \"access\"\n",
            &["--debounce", "30", "-g", "*.conf", "-e", "create", "/tmp"],
        )
        .expect("应加载成功");
        let Action::Run(c) = r else {
            panic!("期望 Run")
        };
        assert_eq!(c.debounce_ms, 30); // 取值型直接被命令行覆盖
        assert_eq!(c.globs, vec!["*.conf"]); // 命令行 -g 替换配置 globs
        assert_eq!(c.paths, vec![PathBuf::from("/tmp")]); // 位置参数替换配置 paths
        assert_eq!(c.event_mask, BIT_CREATE); // -e 整体替换配置 events
    }

    #[test]
    fn cli_globs_accumulate_and_config_paths_survive() {
        // 命令行多个 -g 累积；命令行没给位置参数时配置 paths 保留。
        let r = run_with_config(
            "paths = [\"/etc\", \"/var\"]\n",
            &["-g", "*.log", "--glob=*.conf"],
        )
        .expect("应加载成功");
        let Action::Run(c) = r else {
            panic!("期望 Run")
        };
        assert_eq!(c.globs, vec!["*.log", "*.conf"]);
        assert_eq!(c.paths, vec![PathBuf::from("/etc"), PathBuf::from("/var")]);
    }

    #[test]
    fn booleans_are_additive() {
        // 布尔只增不减：配置开启后，命令行无法关闭。
        let r = run_with_config("recursive = true\nquiet = true\n", &[]).expect("应加载成功");
        let Action::Run(c) = r else {
            panic!("期望 Run")
        };
        assert!(c.recursive && c.quiet);
    }

    #[test]
    fn config_errors_propagate() {
        // 读不到、TOML 语法错、未知键、重复指定 --config：一律 Err（exit 2 路径）。
        assert!(run_with_config("recursiv = true\n", &[]).is_err());
        let args: Vec<String> = vec!["--config".into(), "/nonexistent/x.toml".into()];
        assert!(parse(&args).is_err()); // 文件不存在
        let dup: Vec<String> = vec![
            "--config".into(),
            "/nonexistent/x.toml".into(),
            "--config".into(),
            "/nonexistent/y.toml".into(),
        ];
        assert!(parse(&dup).is_err());
        // --config 缺值/空值走 take_value 报错
        for bad in [vec!["--config"], vec!["--config="]] {
            let args: Vec<String> = bad.iter().map(|s| s.to_string()).collect();
            assert!(parse(&args).is_err(), "应当报错: {bad:?}");
        }
    }

    #[test]
    fn help_and_version() {
        let h: Vec<String> = vec!["-h".into()];
        assert!(matches!(parse(&h), Ok(Action::Help)));
        let v: Vec<String> = vec!["--version".into()];
        assert!(matches!(parse(&v), Ok(Action::Version)));
    }

    #[test]
    fn dash_is_positional() {
        let c = run(&["-"]);
        assert_eq!(c.paths, vec![PathBuf::from("-")]);
    }

    #[test]
    fn config_after_double_dash_is_path() {
        // `--` 终结符之后的一切按位置参数处理，--config 预扫描同样不得越界：
        // `--` 后的 --config=... 是路径，不得被加载配置、也不得从参数里剥离。
        let r = parse(&["--".into(), "--config=evil.toml".into()]).expect("应按路径解析");
        let Action::Run(c) = r else {
            panic!("期望 Run")
        };
        assert_eq!(c.paths, vec![PathBuf::from("--config=evil.toml")]);
    }
}

/// i18n 一致性检查：中英 HELP/USAGE 的 -word token 集合必须相等（与 suoji i18n_tests 同思路）。
#[cfg(test)]
mod i18n_tests {
    use super::{HELP_EN, HELP_ZH, USAGE_EN, USAGE_ZH};
    use std::collections::BTreeSet;

    /// -word token 提取（供一致性检查用，简单扫描）：'-' 后跟 ASCII 字母/下划线视为
    /// token 起点，继续读取 [A-Za-z0-9_]；前导字符为字母/数字/下划线时视为英文
    /// 行文连字符（command-line）不取。中英两版 HELP/USAGE 的 token 集合
    /// 必须相等——防止一边加 flag 另一边漏译。
    /// 已知伪 token：示例/描述行文里的 `sh -c`（→"c"）与 `logger -t`（→"t"）
    /// 会被一并计入；当前中英对称，检查通过。若将来新增真实 -c/-t 选项，
    /// 一侧漏译时会被伪 token 掩盖——届时在断言前排除这两个噪声项。
    fn flag_tokens(s: &str) -> BTreeSet<String> {
        let b = s.as_bytes();
        let mut out = BTreeSet::new();
        let mut i = 0usize;
        while i < b.len() {
            let prev_wordish = i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
            if b[i] == b'-'
                && !prev_wordish
                && i + 1 < b.len()
                && (b[i + 1].is_ascii_alphabetic() || b[i + 1] == b'_')
            {
                let start = i + 1;
                let mut j = start;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
                out.insert(String::from_utf8_lossy(&b[start..j]).into_owned());
                i = j;
            } else {
                i += 1;
            }
        }
        out
    }

    #[test]
    fn help_en_zh_flag_tokens_equal() {
        let en = flag_tokens(HELP_EN);
        let zh = flag_tokens(HELP_ZH);
        let only_en: Vec<_> = en.difference(&zh).collect();
        let only_zh: Vec<_> = zh.difference(&en).collect();
        assert!(
            only_en.is_empty() && only_zh.is_empty(),
            "EN-only: {only_en:?}, ZH-only: {only_zh:?}"
        );
        // 同时确认提取器没有退化：集合非空
        assert!(en.contains("recursive") && en.contains("glob") && en.contains("exec"));
    }

    #[test]
    fn usage_en_zh_flag_tokens_equal() {
        assert_eq!(flag_tokens(USAGE_EN), flag_tokens(USAGE_ZH));
        // fnotify 的 usage 行不含 flag token（与 suoji 不同）；若将来加入，
        // 此断言会提示同步检查两边翻译。
        assert!(flag_tokens(USAGE_EN).is_empty());
    }

    #[test]
    fn usage_is_prefix_of_help() {
        // usage 行 = HELP 首行（EN/ZH 各自成立）
        assert!(HELP_EN.starts_with(USAGE_EN));
        assert!(HELP_ZH.starts_with(USAGE_ZH));
    }
}
