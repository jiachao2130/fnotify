// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! fnotify 入口：解析参数 → 规范化监听路径 → 组装 sink（JSONL + exec）→ 启动引擎。

// 与 lib 同口径：生产代码零 unwrap/expect 交给 lint 强制，测试模块不受约束。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use fnotify::cli::{self, Action};
use fnotify::engine::{self, BatchItem};
use fnotify::locale::pick;
use fnotify::pickf;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn main() {
    // args_os + lossy：非 UTF-8 argv 不 panic（路径随后 canonicalize 失败会以退出码 2 报出）。
    let args: Vec<String> = std::env::args_os()
        .skip(1)
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    let opts = match cli::parse(&args) {
        Ok(Action::Help) => {
            print!("{}", cli::help());
            return;
        }
        Ok(Action::Version) => {
            println!("{}", cli::VERSION);
            return;
        }
        Ok(Action::Run(c)) => c,
        Err(e) => {
            eprintln!("fnotify: {e}");
            eprintln!("{}", cli::usage());
            std::process::exit(2);
        }
    };

    // 监听路径启动时规范化一次，之后全程传 PathBuf；不存在 → 退出码 2。
    // 规范化后去重（同一目录给两次会让摘除计数失衡），并拒绝递归根嵌套
    // 包含（子目录会被递归注册两遍，双 wd 双报；非递归外层下的嵌套根合法）。
    // 校验实现与库 API 共用 engine::check_roots（design §9 同表同拒）。
    let roots = match fnotify::engine::check_roots(&opts.paths, opts.recursive) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("fnotify: {e}");
            std::process::exit(2);
        }
    };

    // webhook 端点启动时校验一次（仅 http），非法即 exit 2——不在事件流里才报错。
    let webhook = match opts.webhook.as_deref() {
        Some(url) => match fnotify::hook::parse_endpoint(url) {
            Ok(ep) => Some(ep),
            Err(e) => {
                eprintln!("fnotify: {e}");
                std::process::exit(2);
            }
        },
        None => None,
    };

    let quiet = opts.quiet;
    let mut pipe_warned = false;
    // exec 生命周期记录进 stdout 结构化流：配置 -x 且未 -q 时启用。
    let log_exec = opts.exec.is_some() && !quiet;
    let pool = Arc::new(Mutex::new(ExecPool::new(
        opts.exec,
        opts.jobs.max(1) as usize,
        Duration::from_secs(opts.timeout_sec),
        log_exec,
    )));
    let pool_sink = Arc::clone(&pool);
    let sink: engine::Sink = Box::new(move |batch: &[BatchItem]| {
        if !quiet {
            let mut out = std::io::stdout().lock();
            for item in batch {
                if writeln!(out, "{}", engine::jsonl_line(item)).is_err() && !pipe_warned {
                    // stdout 关闭（如管道接到 head）：只告警一次，后续写入仍会尝试、失败静默忽略。
                    pipe_warned = true;
                    eprintln!(
                        "fnotify: {}",
                        pick(
                            "stdout write failed (pipe closed?); further failures will not be reported",
                            "stdout 写入失败（管道已关闭？），后续失败不再重复告警"
                        )
                    );
                }
            }
        }
        if let Some(ep) = webhook.as_ref() {
            // 同步推送：与 exec 同一动作 worker，失败仅告警不打断事件流。
            if let Err(e) = fnotify::hook::post_batch(ep, batch) {
                eprintln!("fnotify: {e}");
            }
        }
        if let Ok(p) = pool_sink.lock() {
            for item in batch {
                p.dispatch(item);
            }
        }
    });

    let engine_opts = engine::Options {
        paths: roots,
        recursive: opts.recursive,
        globs: opts.globs,
        excludes: opts.excludes,
        event_mask: opts.event_mask,
        debounce_ms: opts.debounce_ms,
        flush_immediate: opts.flush_immediate,
        oneshot: opts.oneshot,
        until_idle: opts.until_idle.map(Duration::from_secs),
        ready_file: opts.ready_file,
    };
    if let Err(e) = engine::run(engine_opts, sink) {
        eprintln!("fnotify: {e}");
        // 引擎以错误退出时同样收割在跑 exec（超时杀仍生效），不留孤儿进程。
        if let Ok(p) = pool.lock() {
            p.finish();
        }
        std::process::exit(1);
    }
    // 优雅停机收尾：等在跑的 exec 结束（超时仍生效——超龄者杀）。
    let Ok(p) = pool.lock() else {
        return;
    };
    p.finish();
}

/// exec 进程池：模板按 `%p`/`%e` 替换后经 `sh -c` 执行，最多 `jobs` 个并发在跑，
/// 单个进程超 `timeout`（0 = 不限）即杀。超时由后台回收线程按时钟驱动——事件流
/// 静默期间同样在跑进程也会到点被杀，不依赖新事件或停机来触发回收。
/// 池满时新事件在 `dispatch` 内背压等待。
struct ExecPool {
    inner: Arc<Mutex<PoolInner>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    reaper: Option<std::thread::JoinHandle<()>>,
}

/// 池内共享状态：事件线程（dispatch）与回收线程（reap）经同一把锁互斥访问。
struct PoolInner {
    tpl: Option<String>,
    jobs: usize,
    timeout: Duration,
    /// 是否向 stdout 写 exec 生命周期记录（配置 -x 且未 -q）。
    log: bool,
    running: Vec<RunningExec>,
}

struct RunningExec {
    child: Child,
    started: Instant,
    /// 渲染后的命令字节（可能含非 UTF-8 路径），展示面统一 lossy。
    cmd: Vec<u8>,
    /// 已被超时组杀：回收时不再重复打非零退出告警。
    killed: bool,
}

impl ExecPool {
    fn new(tpl: Option<String>, jobs: usize, timeout: Duration, log: bool) -> Self {
        let has_tpl = tpl.is_some();
        let inner = Arc::new(Mutex::new(PoolInner {
            tpl,
            jobs: jobs.max(1),
            timeout,
            log: log && has_tpl,
            running: Vec::new(),
        }));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // 后台回收线程：50ms 一拍，到点回收退出进程、组杀超龄进程——
        // 事件流静默时也没有东西依赖新事件来驱动超时判定。
        // 未配置 exec 模板时没有进程可收，不起线程。
        let reaper = if has_tpl {
            let inner = Arc::clone(&inner);
            let stop = Arc::clone(&stop);
            Some(std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Ok(mut p) = inner.lock() {
                        p.reap();
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }))
        } else {
            None
        };
        ExecPool {
            inner,
            stop,
            reaper,
        }
    }

    /// 投递一条事件的 exec；池满时背压等待空位。
    fn dispatch(&self, item: &BatchItem) {
        let Ok(mut p) = self.inner.lock() else {
            return;
        };
        let Some(tpl) = p.tpl.as_deref() else {
            return;
        };
        let cmd = render_template(tpl, item);
        p.reap();
        while p.running.len() >= p.jobs {
            // 背压：等一个进程退出（超时杀仍在 reap 内生效）。
            std::thread::sleep(Duration::from_millis(10));
            p.reap();
        }
        match Command::new("sh")
            .arg("-c")
            .arg(std::ffi::OsStr::from_bytes(&cmd))
            // 独立进程组：超时杀要带走 sh -c 派生的孙进程，不能只杀 sh 本体。
            .process_group(0)
            .spawn()
        {
            Ok(child) => {
                let pid = child.id();
                if p.log {
                    emit_record(&engine::exec_start_line(
                        engine::now_ms(),
                        pid,
                        &String::from_utf8_lossy(&cmd),
                    ));
                }
                p.running.push(RunningExec {
                    child,
                    started: Instant::now(),
                    cmd,
                    killed: false,
                });
            }
            Err(e) => eprintln!(
                "fnotify: {}",
                pickf!("exec spawn failed: {e}", "exec 启动失败: {e}", e = e)
            ),
        }
    }

    /// 停机收尾：等在跑进程清空（超时杀仍生效）。timeout=0 且进程不退出时会
    /// 一直等——与串行语义下"队列不清完不停机"一致。
    fn finish(&self) {
        loop {
            {
                let Ok(mut p) = self.inner.lock() else {
                    return;
                };
                p.reap();
                if p.running.is_empty() {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ExecPool {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.reaper.take() {
            let _ = h.join();
        }
    }
}

impl PoolInner {
    /// 回收已退出的进程；超龄者按进程组杀（含 sh -c 派生的孙进程）再回收。非阻塞。
    fn reap(&mut self) {
        let timeout = self.timeout;
        let mut i = 0;
        while i < self.running.len() {
            // 退出与超时判定的先后是关键：已退出的进程即使超龄也按正常退出回收，
            // 不误报超时（`sleep 0.1` 配 100ms 超时不该打出超时告警）。
            // 三态：None = 在跑；Some(None) = 已退出但状态不可得；Some(Some) = 已退出。
            let state: Option<Option<std::process::ExitStatus>> =
                match self.running[i].child.try_wait() {
                    Ok(Some(status)) => Some(Some(status)),
                    Ok(None) => None,
                    Err(_) => Some(None), // wait 失败（已退出等）：按已退出回收
                };
            match state {
                Some(status) => {
                    if let Some(status) = status {
                        // 被超时组杀的进程退出码必为信号非零，超时告警已打过，不再重复。
                        if !status.success() && !self.running[i].killed {
                            let st = status;
                            let cmd = String::from_utf8_lossy(&self.running[i].cmd);
                            eprintln!(
                                "fnotify: {}",
                                pickf!(
                                    "exec exit status {st}: {cmd}",
                                    "exec 退出码 {st}: {cmd}",
                                    st = st,
                                    cmd = cmd
                                )
                            );
                        }
                    }
                    if self.log {
                        // exit/signal 依退出形态取值；状态不可得均为 null。
                        let (exit, signal) = match status {
                            Some(st) => (st.code(), st.signal()),
                            None => (None, None),
                        };
                        let r = &self.running[i];
                        let line = engine::exec_end_line(
                            engine::now_ms(),
                            r.child.id(),
                            &String::from_utf8_lossy(&r.cmd),
                            exit,
                            signal,
                            r.killed,
                            r.started.elapsed().as_millis(),
                        );
                        emit_record(&line);
                    }
                    let _ = self.running[i].child.wait();
                    self.running.swap_remove(i);
                }
                None => {
                    let over =
                        timeout > Duration::ZERO && self.running[i].started.elapsed() > timeout;
                    if over {
                        let cmd = String::from_utf8_lossy(&self.running[i].cmd);
                        eprintln!(
                            "fnotify: {}",
                            pickf!(
                                "exec timed out after {sec}s: {cmd}",
                                "exec 超时（{sec}s）: {cmd}",
                                sec = timeout.as_secs(),
                                cmd = cmd
                            )
                        );
                        kill_process_group(&mut self.running[i].child);
                        self.running[i].killed = true;
                    }
                    i += 1;
                }
            }
        }
    }
}

/// 生命周期记录写入 stdout（与事件 JSONL 同流）。管道已关闭时静默——
/// sink 对 JSONL 写过失败已告警过一次，这里不重复。
fn emit_record(line: &str) {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
}

/// 按进程组杀：子进程经 `process_group(0)` 自成一组且任组长（pgid = pid），
/// 杀负 pgid 即带走 sh -c 派生的全部孙进程，不留孤儿。直调 libc::kill——
/// libc 本就在依赖树中（inotify 传递依赖），成为直接依赖只为这一个调用。
fn kill_process_group(child: &mut Child) {
    // 安全前提：pgid 取自本进程刚 spawn 的子进程，必为活进程组；ESRCH（已退出）
    // 无害。kill(1) 外部命令版曾以「无 libc 依赖」为理由，但该理由不成立。
    let pgid = child.id() as libc::pid_t;
    let rc = unsafe { libc::kill(-pgid, libc::SIGKILL) };
    if rc != 0 {
        // 杀组失败（极罕见）时退回只杀直接子进程。
        let _ = child.kill();
    }
}

/// 按 `%p`（事件路径）/ `%e`（事件名）替换模板。路径以 shell 单引号包裹，
/// 杜绝文件名内容注入 shell 语法。返回原始字节而非 String：非 UTF-8 文件名
/// 经 to_string_lossy 会变成 U+FFFD 假路径，exec 将作用在错误的文件上——
/// 单引号包裹的字节段 sh 原样直通（Linux 路径无 NUL，字节级引用安全）。
fn render_template(tpl: &str, item: &BatchItem) -> Vec<u8> {
    let mut cmd = Vec::with_capacity(tpl.len() + 16);
    let mut chars = tpl.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            cmd.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
            continue;
        }
        match chars.peek() {
            Some('p') => {
                chars.next();
                cmd.extend_from_slice(&shell_quote_path(&item.path));
            }
            Some('e') => {
                chars.next();
                cmd.extend_from_slice(item.event.as_bytes());
            }
            _ => cmd.push(b'%'), // 孤立的 % 原样保留
        }
    }
    cmd
}

/// POSIX 单引号包裹路径**原始字节**：`it's` → `'it'\''s'`；非 UTF-8 字节
/// 原样保留（%p 渲染走这里，exec 拿到的是字节级真路径，展示面才用 lossy）。
fn shell_quote_path(path: &std::path::Path) -> Vec<u8> {
    let bytes = path.as_os_str().as_bytes();
    let mut out = Vec::with_capacity(bytes.len() + 2);
    out.push(b'\'');
    for &b in bytes {
        if b == b'\'' {
            out.extend_from_slice(b"'\\''");
        } else {
            out.push(b);
        }
    }
    out.push(b'\'');
    out
}

#[cfg(test)]
mod tests {
    use super::{BatchItem, ExecPool, render_template, shell_quote_path};
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    fn item() -> BatchItem {
        BatchItem {
            ts: 0,
            event: "create",
            path: PathBuf::from("/tmp/fnotify-pool-test"),
            dir: false,
        }
    }

    #[test]
    fn shell_quote_pins_posix_single_quoting() {
        let q = |v: Vec<u8>| String::from_utf8(v).unwrap();
        // 单引号内嵌套：' → '\''
        assert_eq!(q(shell_quote_path(&PathBuf::from("it's"))), "'it'\\''s'");
        // 空格：整体包裹，不分词
        assert_eq!(q(shell_quote_path(&PathBuf::from("a b.log"))), "'a b.log'");
        // 命令替换 / 分号：均在引号内，是字面量
        assert_eq!(
            q(shell_quote_path(&PathBuf::from("$(touch x)"))),
            "'$(touch x)'"
        );
        assert_eq!(q(shell_quote_path(&PathBuf::from(";"))), "';'");
        // 空串：一对空引号，仍是安全的单个空参数
        assert_eq!(q(shell_quote_path(&PathBuf::from(""))), "''");
    }

    #[test]
    fn render_template_passes_non_utf8_path_bytes_verbatim() {
        // 非 UTF-8 文件名（0xff 字节）：%p 必须原样直通——旧版经
        // to_string_lossy 变成 U+FFFD 假路径，exec 会作用在错误文件上。
        let raw = b"/tmp/fnotify-\xff.bin".to_vec();
        let item = BatchItem {
            path: PathBuf::from(OsString::from_vec(raw.clone())),
            ..item()
        };
        let cmd = render_template("cat %p", &item);
        assert_eq!(
            cmd,
            [
                b"cat ".as_slice(),
                b"'".as_slice(),
                raw.as_slice(),
                b"'".as_slice()
            ]
            .concat(),
            "非 UTF-8 路径字节必须原样出现在命令字节里：{cmd:?}"
        );
    }

    #[test]
    fn render_template_event_and_literal_percent() {
        let cmd = render_template("%e: %p %%", &item());
        assert_eq!(
            String::from_utf8(cmd).unwrap(),
            "create: '/tmp/fnotify-pool-test' %%"
        );
    }

    #[test]
    fn exec_pool_reaps_finished_children() {
        let p = ExecPool::new(Some("sleep 0.2".into()), 4, Duration::ZERO, false);
        p.dispatch(&item());
        assert_eq!(
            p.inner.lock().unwrap().running.len(),
            1,
            "应已入池一个在跑进程"
        );
        p.finish();
        assert!(
            p.inner.lock().unwrap().running.is_empty(),
            "finish 应收空在跑进程"
        );
    }

    #[test]
    fn exec_pool_backpressure_when_full() {
        // 池满时 dispatch 背压等待空位：jobs=1 下第二个 dispatch 的耗时
        // 应不小于第一个进程的运行时长（0.3s）。
        let p = ExecPool::new(Some("sleep 0.3".into()), 1, Duration::ZERO, false);
        p.dispatch(&item());
        let t0 = Instant::now();
        p.dispatch(&item());
        assert!(
            t0.elapsed() >= Duration::from_millis(250),
            "池满应背压等待，实际 {:?}",
            t0.elapsed()
        );
        p.finish();
    }

    #[test]
    fn exec_pool_timeout_kills_and_finishes_fast() {
        let p = ExecPool::new(
            Some("sleep 30".into()),
            2,
            Duration::from_millis(200),
            false,
        );
        p.dispatch(&item());
        let t0 = Instant::now();
        p.finish();
        assert!(p.inner.lock().unwrap().running.is_empty(), "超时杀应收空");
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "超时杀后不应久等，实际 {:?}",
            t0.elapsed()
        );
    }

    #[test]
    fn exec_pool_timeout_kills_the_whole_process_group() {
        // 孙进程不留孤儿：后台子 shell 计划 1s 后落 sentinel；组杀应让它到不了。
        let n = std::process::id();
        let sentinel = std::env::temp_dir().join(format!("fnotify-pgkill-{n}"));
        let _ = std::fs::remove_file(&sentinel);
        let tpl = format!("(sleep 1; touch {}) & exec sleep 30", sentinel.display());
        let p = ExecPool::new(Some(tpl), 1, Duration::from_millis(200), false);
        p.dispatch(&item());
        let t0 = Instant::now();
        p.finish();
        assert!(p.inner.lock().unwrap().running.is_empty());
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "组杀后应收场，实际 {:?}",
            t0.elapsed()
        );
        std::thread::sleep(Duration::from_millis(1500)); // 等假想的孤儿窗口过去
        assert!(!sentinel.exists(), "孙进程应随组杀消亡，sentinel 不应出现");
    }

    #[test]
    fn exec_pool_reaper_gated_on_template() {
        // 未配置 exec 模板时不起回收线程：没有进程可收，零后台开销。
        let p = ExecPool::new(None, 1, Duration::from_secs(1), false);
        assert!(p.reaper.is_none());
        // dispatch 在无模板时直接返回，也不触发起线程之外的任何路径。
        p.dispatch(&item());
        assert!(p.inner.lock().unwrap().running.is_empty());
        // 配置模板则起线程。
        let p = ExecPool::new(Some("true".into()), 1, Duration::ZERO, false);
        assert!(p.reaper.is_some());
        p.finish();
    }

    #[test]
    fn exec_pool_timeout_enforced_during_idle() {
        // 静默期超时仍生效：dispatch 之后不派新事件、不调 finish，超龄进程
        // 也必须被组杀——孙进程到点要写的 sentinel 绝不能落盘。
        let n = std::process::id();
        let sentinel = std::env::temp_dir().join(format!("fnotify-idle-{n}"));
        let _ = std::fs::remove_file(&sentinel);
        let tpl = format!("(sleep 1; touch {}) & exec sleep 30", sentinel.display());
        let p = ExecPool::new(Some(tpl), 1, Duration::from_millis(200), false);
        p.dispatch(&item());
        // 纯静默等待：窗口覆盖孙进程计划落盘的 1s 时刻，若超时失效它必出现。
        std::thread::sleep(Duration::from_millis(1500));
        assert!(
            p.inner.lock().unwrap().running.is_empty(),
            "静默期也应回收被杀进程"
        );
        assert!(
            !sentinel.exists(),
            "静默期超时杀应带走孙进程，sentinel 不应出现"
        );
        p.finish();
    }
}
