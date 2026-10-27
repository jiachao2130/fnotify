// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! 集成测试：s1–s34 共 34 个场景，直连内核 inotify。
//!
//! 断言策略：
//! - 用「窗口内收到包含某事件」而非严格序列；按文件名过滤后再断言。
//! - 等待超时统一 2 秒（inotify 投递及时，2s 约为 40 个默认窗口，裕量充足）。
//! - 操作之间 sleep 400ms；负断言用屏障法（先制造应被过滤的事件，等到
//!   之后发生的通过事件后再断言前者缺席），避免时序抖动造成假绿。

use fnotify::engine::{self, BatchItem, Options};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const START_SETTLE: Duration = Duration::from_millis(600); // 等 watcher 建立完毕
const BETWEEN_OPS: Duration = Duration::from_millis(400); // 操作间隔（>300ms）
const WAIT: Duration = Duration::from_secs(2); // 断言窗口（约为 40 个默认去抖窗口）

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

/// RAII 临时目录：唯一名（pid + 序号 + tag），测试结束递归删除。
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> Scratch {
    let n = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!("fnotify-it-{}-{}-{}", std::process::id(), n, tag));
    fs::create_dir_all(&d).expect("创建临时目录");
    Scratch(d)
}

/// RAII 引擎：Drop 时置 shutdown 标志并 join 引擎线程。
struct Engine {
    rx: mpsc::Receiver<BatchItem>,
    err_rx: mpsc::Receiver<String>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// 后台线程跑 run_with_shutdown（不经 Signals），事件批次逐条送进 mpsc。
fn start(opts: Options) -> Engine {
    let (tx, rx) = mpsc::channel::<BatchItem>();
    let (etx, err_rx) = mpsc::channel::<String>();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let handle = std::thread::spawn(move || {
        let sink: engine::Sink = Box::new(move |batch: &[BatchItem]| {
            for item in batch {
                let _ = tx.send(item.clone());
            }
        });
        if let Err(e) = engine::run_with_shutdown(opts, sink, flag) {
            let _ = etx.send(e);
        }
    });
    Engine {
        rx,
        err_rx,
        stop,
        handle: Some(handle),
    }
}

fn opts(dir: &Path, recursive: bool, globs: Vec<&str>, debounce_ms: u64) -> Options {
    Options {
        paths: vec![dir.to_path_buf()],
        recursive,
        globs: globs.iter().map(|g| g.to_string()).collect(),
        excludes: vec![],
        oneshot: false,
        until_idle: None,
        ready_file: None,
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms,
        flush_immediate: false,
    }
}

/// immediate 模式的便捷构造（s13/s14 专用）。
fn opts_imm(dir: &Path, debounce_ms: u64) -> Options {
    Options {
        paths: vec![dir.to_path_buf()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        oneshot: false,
        until_idle: None,
        ready_file: None,
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms,
        flush_immediate: true,
    }
}

fn file_name_of(p: &Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn engine_err_hint(e: &Engine) -> String {
    match e.err_rx.try_recv() {
        Ok(msg) => format!("（引擎已报错退出: {msg}）"),
        Err(_) => String::new(),
    }
}

/// 在 deadline 内等待「文件名 == name 且事件 == event」的事件；容忍无关事件与同文件多报。
/// 返回匹配到的事件条目（调用方可进一步断言路径等字段）。
fn expect_event(e: &Engine, event: &str, name: &str) -> BatchItem {
    let deadline = Instant::now() + WAIT;
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            panic!(
                "{WAIT:?} 内未等到 {name} 的 {event} 事件{}",
                engine_err_hint(e)
            );
        }
        match e.rx.recv_timeout(remain) {
            Ok(it) => {
                if it.event == event && file_name_of(&it.path) == name {
                    return it;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "{WAIT:?} 内未等到 {name} 的 {event} 事件{}",
                    engine_err_hint(e)
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("引擎提前退出{}", engine_err_hint(e));
            }
        }
    }
}

/// 收集窗口内文件名为 name 的全部事件（含多批）。
fn collect_events(e: &Engine, name: &str, window: Duration) -> Vec<BatchItem> {
    let deadline = Instant::now() + window;
    let mut out = Vec::new();
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            return out;
        }
        match e.rx.recv_timeout(remain) {
            Ok(it) => {
                if file_name_of(&it.path) == name {
                    out.push(it);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("引擎提前退出，无法继续收集 {name}{}", engine_err_hint(e));
            }
        }
    }
}

fn sleep(d: Duration) {
    std::thread::sleep(d);
}

/// 场景 1：非递归监听单目录，create → modify → remove 全生命周期；
/// 顺带锚定 dir 字段取自内核 IN_ISDIR（文件 false、目录 true）。
#[test]
fn s1_non_recursive_lifecycle() {
    let dir = scratch("s1");
    let e = start(opts(&dir.0, false, vec![], 50));
    sleep(START_SETTLE);

    let f = dir.0.join("a.log");
    fs::write(&f, b"v1").unwrap();
    let it = expect_event(&e, "create", "a.log");
    assert!(!it.dir, "文件的 create 事件 dir 应为 false");

    sleep(BETWEEN_OPS);
    fs::write(&f, b"v2").unwrap();
    expect_event(&e, "modify", "a.log");

    sleep(BETWEEN_OPS);
    fs::remove_file(&f).unwrap();
    expect_event(&e, "remove", "a.log");

    // 目录事件的 dir 标志
    sleep(BETWEEN_OPS);
    fs::create_dir(dir.0.join("d")).unwrap();
    let it = expect_event(&e, "create", "d");
    assert!(it.dir, "目录的 create 事件 dir 应为 true（IN_ISDIR）");
}

/// 场景 2：-g '*.log' 只放行 .log——屏障法负断言：先写 b.txt（应被过滤），
/// 再写 c.log（应通过）；等到 c.log 事件这一屏障后，断言途中从未出现 b.txt 事件。
/// b.txt 早于 c.log 写入，若泄漏必在 c.log 之前/同批到达，消除投递时序造成的假绿。
#[test]
fn s2_glob_filters_out_non_matching() {
    let dir = scratch("s2");
    let e = start(opts(&dir.0, false, vec!["*.log"], 50));
    sleep(START_SETTLE);

    fs::write(dir.0.join("a.log"), b"x").unwrap();
    expect_event(&e, "create", "a.log");

    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("b.txt"), b"x").unwrap();
    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("c.log"), b"x").unwrap();

    // 屏障：等到 c.log 的 create；期间任何 b.txt 事件都直接判失败。
    let deadline = Instant::now() + WAIT;
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            panic!(
                "{WAIT:?} 内未等到 c.log 的 create 事件{}",
                engine_err_hint(&e)
            );
        }
        match e.rx.recv_timeout(remain) {
            Ok(it) => {
                let name = file_name_of(&it.path);
                assert_ne!(name, "b.txt", "b.txt 应被 *.log 过滤，却收到 {}", it.event);
                if name == "c.log" && it.event == "create" {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "{WAIT:?} 内未等到 c.log 的 create 事件{}",
                    engine_err_hint(&e)
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("引擎提前退出{}", engine_err_hint(&e));
            }
        }
    }
    // 收尾：排空缓冲，同批/紧随其后的残留也不得含 b.txt。
    while let Ok(it) = e.rx.try_recv() {
        assert_ne!(file_name_of(&it.path), "b.txt", "屏障后仍收到 b.txt 的事件");
    }
}

/// 场景 3：去抖——快速连写同一文件 5 次，只应聚出 1 个 modify 批次条目。
///
/// 连写总时长 150ms（30ms × 5）必须小于去抖窗口（200ms），
/// 否则批次时长封顶会把一次连写切成两批。
#[test]
fn s3_debounce_collapses_burst() {
    let dir = scratch("s3");
    let f = dir.0.join("burst.log");
    fs::write(&f, b"seed").unwrap(); // 预创建；断言只看 modify
    let e = start(opts(&dir.0, false, vec![], 200));
    sleep(START_SETTLE);

    for i in 0..5 {
        fs::write(&f, format!("v{i}")).unwrap();
        sleep(Duration::from_millis(30)); // 30ms × 5 = 150ms 连写 < 200ms 窗口
    }
    let got = collect_events(&e, "burst.log", Duration::from_secs(2));
    let modifies = got.iter().filter(|it| it.event == "modify").count();
    assert_eq!(
        modifies, 1,
        "5 连写应去抖为 1 个 modify，实际收到 {modifies}: {got:?}"
    );
}

/// 场景 4：-r 递归——启动前已存在的子目录里创建文件也能收到。
#[test]
fn s4_recursive_sees_existing_subdir() {
    let dir = scratch("s4");
    let sub = dir.0.join("sub");
    fs::create_dir(&sub).unwrap(); // 启动前已存在
    let e = start(opts(&dir.0, true, vec![], 50));
    sleep(START_SETTLE);

    fs::write(sub.join("deep.log"), b"x").unwrap();
    expect_event(&e, "create", "deep.log");
}

/// 场景 5（回归）：繁忙流——写间隔持续小于去抖窗口时，
/// 修复前 recv_timeout 永不超时，批次无限延迟、shutdown 永不生效。
/// 断言：预置 shutdown 后引擎在有界时间内退出，且期间已交付过至少一批事件。
#[test]
fn s5_busy_stream_shutdown_is_bounded() {
    let dir = scratch("s5");
    let f = dir.0.join("busy.log");
    fs::write(&f, b"seed").unwrap();
    let mut e = start(opts(&dir.0, false, vec![], 100)); // 100ms 窗口
    sleep(START_SETTLE);

    // 写手线程：10ms 间隔连写（<< 100ms 窗口），10s 硬上限防止线程泄漏。
    let writer_stop = Arc::new(AtomicBool::new(false));
    let wflag = writer_stop.clone();
    let wpath = f.clone();
    let writer = std::thread::spawn(move || {
        let hard_stop = Instant::now() + Duration::from_secs(10);
        for i in 0.. {
            if wflag.load(Ordering::Relaxed) || Instant::now() >= hard_stop {
                break;
            }
            let _ = fs::write(&wpath, format!("v{i}"));
            sleep(Duration::from_millis(10));
        }
    });

    // 停机前必须等到第一批中途交付（这是时长封顶在 Ok 分支生效的直接证据）。
    // 等第一批而不是固定 sleep——固定时长可能一批都还没到。
    let mut early = 0usize;
    let first_batch_deadline = Instant::now() + WAIT;
    while early == 0 {
        while let Ok(it) = e.rx.try_recv() {
            if file_name_of(&it.path) == "busy.log" {
                early += 1;
            }
        }
        assert!(
            Instant::now() < first_batch_deadline,
            "繁忙流跑了 {WAIT:?} 仍无中途交付（时长封顶未接入？）"
        );
        sleep(Duration::from_millis(20));
    }
    e.stop.store(true, Ordering::Release); // 预置 shutdown

    let handle = e.handle.take().expect("引擎线程句柄");
    let deadline = Instant::now() + Duration::from_secs(3); // 有界退出
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "繁忙流下引擎未在 3s 内退出（停机失效）"
        );
        sleep(Duration::from_millis(20));
    }
    let _ = handle.join();

    // 交付断言：批次时长封顶保证繁忙流也持续交付，≥1 批（而非无限延迟）。
    let mut delivered = early;
    while let Ok(it) = e.rx.try_recv() {
        if file_name_of(&it.path) == "busy.log" {
            delivered += 1;
        }
    }
    assert!(delivered >= 1, "繁忙流下应已交付至少一批事件，实际 0 批");

    writer_stop.store(true, Ordering::Relaxed);
    let _ = writer.join();
}

/// 场景 6：CLI 端到端——真实二进制 `-x` 的 %p 占位符经 sh 执行写入结果文件
/// （含恶意文件名字节级断言）；随后删除监听目录，fnotify 应优雅退出且退出码 0（目录删除退出语义）。
#[test]
fn s6_exec_placeholder_and_root_removal_exit() {
    let dir = scratch("s6");
    let result = std::env::temp_dir().join(format!(
        "fnotify-it-exec-{}-{}.out",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_file(&result);

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-q")
        .arg("-x")
        .arg(format!("printf %s %p > '{}'", result.display()))
        .arg(&dir.0)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("启动 fnotify 二进制");
    sleep(START_SETTLE);

    // 写被监听文件 → 等待 exec 把 %p 写进结果文件
    fs::write(dir.0.join("x.log"), b"v").unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        if let Ok(s) = fs::read_to_string(&result) {
            assert!(
                !s.is_empty() && s.contains("x.log"),
                "exec 结果文件内容异常: {s:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{WAIT:?} 内 exec 未把 %p 写入结果文件"
        );
        sleep(Duration::from_millis(200));
    }

    // 恶意文件名端到端：空格 + 单引号 + 双引号 + 分号。%p 经 shell 单引号包裹，
    // exec 写出的结果文件内容必须逐字节等于该文件的事件路径（文件名原样嵌入其中）。
    // 期望值取 canonicalize 后的监听目录（与二进制同口径）。
    sleep(BETWEEN_OPS);
    let evil_name = "inj ;'q\".log";
    fs::write(dir.0.join(evil_name), b"v").unwrap();
    let evil_path = fs::canonicalize(&dir.0).unwrap().join(evil_name);
    let evil_bytes = evil_path.as_os_str().as_encoded_bytes().to_vec();
    let deadline = Instant::now() + WAIT;
    loop {
        if let Ok(bytes) = fs::read(&result) {
            if bytes == evil_bytes {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{WAIT:?} 内 exec 未把恶意文件名逐字节写入结果文件（注入或分词即失配）"
        );
        sleep(Duration::from_millis(200));
    }

    // 删除监听目录 → fnotify 检测到并优雅退出（exit 0）
    fs::remove_dir_all(&dir.0).unwrap();
    // 退出预算放宽到 10s：高负载主机（安全软件扫描、并行测试抢 CPU）下
    // 进程调度可能把优雅退出拖到数秒，2s 在空闲机裕量充足但在负载机上误报。
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                assert!(
                    status.success(),
                    "目录删除后应优雅退出 exit 0，实际 {status}"
                );
                break;
            }
            None => {
                assert!(
                    Instant::now() < deadline,
                    "删除监听目录后 10s 内 fnotify 未退出"
                );
                sleep(Duration::from_millis(100));
            }
        }
    }

    // -q 负断言：JSONL 已关闭，stdout 必须为空。
    use std::io::Read;
    let mut out = Vec::new();
    child
        .stdout
        .take()
        .expect("stdout 已接管")
        .read_to_end(&mut out)
        .unwrap();
    assert!(
        out.is_empty(),
        "-q 下 stdout 应为空，实际 {} 字节",
        out.len()
    );
    let _ = fs::remove_file(&result);
}

/// 场景 7：多目录监听 + 递归模式下运行中新建子目录被接管——
/// 两个监听目录都注册生效（dir_b 直接写文件可收到）；
/// dir_a 下启动后新建的 sub/ 里的文件也能收到（递归接管新目录）。
#[test]
fn s7_multi_root_and_runtime_subdir_takeover() {
    let dir_a = scratch("s7a");
    let dir_b = scratch("s7b");
    let o = Options {
        paths: vec![dir_a.0.clone(), dir_b.0.clone()],
        recursive: true,
        globs: vec![],
        excludes: vec![],
        oneshot: false,
        until_idle: None,
        ready_file: None,
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms: 50,
        flush_immediate: false,
    };
    let e = start(o);
    sleep(START_SETTLE);

    // 第二个监听目录确实生效
    fs::write(dir_b.0.join("in-b.log"), b"x").unwrap();
    expect_event(&e, "create", "in-b.log");

    // 启动后才在 dir_a 下新建的子目录，写文件同样收到
    sleep(BETWEEN_OPS);
    let sub = dir_a.0.join("sub");
    fs::create_dir(&sub).unwrap();
    sleep(BETWEEN_OPS); // 新建子目录的接管在同一读取批次内完成，等一拍再写，防竞态假红
    fs::write(sub.join("new-sub.log"), b"x").unwrap();
    expect_event(&e, "create", "new-sub.log");
}

/// 场景 8：rename 语义——inotify 不做 cookie 配对，一次 mv 固定产出两条：
/// 树内 mv 得 rename_from（旧路径）+ rename_to（新路径）；
/// mv 出监听树只有 rename_from，mv 入只有 rename_to；
/// mv 入的目录随后修改文件，modify 必须落在新路径上。
#[test]
fn s8_rename_split_from_to() {
    let dir = scratch("s8");
    let e = start(opts(&dir.0, true, vec![], 50));
    sleep(START_SETTLE);

    // 树内 mv：a.log → b.log
    fs::write(dir.0.join("a.log"), b"x").unwrap();
    expect_event(&e, "create", "a.log");
    sleep(BETWEEN_OPS);
    fs::rename(dir.0.join("a.log"), dir.0.join("b.log")).unwrap();
    let from = expect_event(&e, "rename_from", "a.log");
    assert!(from.path.ends_with("a.log"));
    let to = expect_event(&e, "rename_to", "b.log");
    assert!(to.path.ends_with("b.log"));

    // mv 出监听树：rename_from 之后该路径不再产生事件
    sleep(BETWEEN_OPS);
    let outside = scratch("s8ext");
    fs::rename(dir.0.join("b.log"), outside.0.join("gone.log")).unwrap();
    expect_event(&e, "rename_from", "b.log");

    // mv 入监听树：移入的目录被递归接管，后续 modify 落在新路径
    sleep(BETWEEN_OPS);
    let src = scratch("s8src");
    fs::create_dir(src.0.join("inner")).unwrap();
    fs::rename(src.0.join("inner"), dir.0.join("moved-in")).unwrap();
    expect_event(&e, "rename_to", "moved-in");
    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("moved-in").join("deep.log"), b"y").unwrap();
    let it = expect_event(&e, "modify", "deep.log");
    assert!(
        it.path.ends_with(Path::new("moved-in").join("deep.log")),
        "mv 入目录后的 modify 应落在新路径，实际 {}",
        it.path.display()
    );
}

/// 场景 9（回归）：已接管的子目录被 mv 出监听树——
/// 移出时整棵旧 watch（含后代目录）被摘除，引擎不崩、不以过期路径误报，
/// 树内其余部分照常工作。anotify 当年死于这类有状态维护，此处固定行为。
/// 后代层是钉子：只移出顶层目录不摘后代 watch 的实现会以旧路径误报
/// escaped/deep/post.log。
#[test]
fn s9_dir_moved_out_keeps_engine_alive() {
    let dir = scratch("s9");
    let sub = dir.0.join("escaping");
    fs::create_dir_all(sub.join("deep")).unwrap(); // escaping + escaping/deep 均被接管
    let e = start(opts(&dir.0, true, vec![], 50));
    sleep(START_SETTLE);

    let ext = scratch("s9ext");
    fs::rename(&sub, ext.0.join("escaped")).unwrap();
    sleep(BETWEEN_OPS);

    // 移出的子树后代目录里再写文件：不应以过期路径出现在事件流（屏障法负断言）
    fs::write(ext.0.join("escaped").join("deep").join("post.log"), b"x").unwrap();
    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("still.log"), b"x").unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            panic!(
                "{WAIT:?} 内未等到 still.log 的 create 事件{}",
                engine_err_hint(&e)
            );
        }
        match e.rx.recv_timeout(remain) {
            Ok(it) => {
                assert_ne!(
                    file_name_of(&it.path),
                    "post.log",
                    "移出目录后仍收到其内部文件的事件（过期路径残留）"
                );
                if it.event == "create" && file_name_of(&it.path) == "still.log" {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "{WAIT:?} 内未等到 still.log 的 create 事件{}",
                    engine_err_hint(&e)
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("引擎提前退出{}", engine_err_hint(&e));
            }
        }
    }
}

/// 场景 10（回归）：树内目录 rename 的 cookie 配对——已接管子树的内核 watch
/// 随 inode 存活，映射按前缀改写：rename 后写入落新路径；旧路径重建空目录后，
/// 其下新建子目录仍被接管（stale 反向映射不得挡路）。
#[test]
fn s10_in_tree_rename_remap_and_rebuild_takeover() {
    let dir = scratch("s10");
    let sub = dir.0.join("sub");
    fs::create_dir_all(sub.join("deep")).unwrap(); // sub + sub/deep 均被预接管
    let e = start(opts(&dir.0, true, vec![], 50));
    sleep(START_SETTLE);

    // 树内 rename：sub → sub2
    fs::rename(&sub, dir.0.join("sub2")).unwrap();
    expect_event(&e, "rename_to", "sub2");

    // rename 后写入旧子树：事件路径必须是新前缀
    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("sub2").join("deep").join("f.log"), b"x").unwrap();
    let it = expect_event(&e, "create", "f.log");
    assert!(
        it.path.starts_with(dir.0.join("sub2")),
        "树内 rename 后事件路径应为新前缀，实际 {}",
        it.path.display()
    );

    // 旧路径重建：sub/deep 的 stale 反向映射不得挡住新子目录的接管
    sleep(BETWEEN_OPS);
    fs::create_dir(&sub).unwrap();
    expect_event(&e, "create", "sub");
    sleep(BETWEEN_OPS);
    fs::create_dir(sub.join("deep")).unwrap();
    sleep(BETWEEN_OPS); // 接管在同一读取批次内完成，等一拍再写（同 s7）
    fs::write(sub.join("deep").join("g.log"), b"x").unwrap();
    expect_event(&e, "create", "g.log");
}

/// 场景 11（回归）：动作 worker 崩溃（sink panic）后引擎不空转——
/// flush 发送失败即停止监听，以 Err 退出（exit 1 路径的引擎侧锚定）。
#[test]
fn s11_worker_panic_stops_engine() {
    let dir = scratch("s11");
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let o = opts(&dir.0, false, vec![], 50);
    let handle = std::thread::spawn(move || {
        let sink: engine::Sink = Box::new(|_| panic!("sink boom（测试预期内的 panic）"));
        engine::run_with_shutdown(o, sink, flag)
    });
    sleep(START_SETTLE);

    // 第一批：worker 收到后 panic；第二批：flush 发送失败 → 引擎应以 Err 收尾。
    fs::write(dir.0.join("x.log"), b"v").unwrap();
    sleep(BETWEEN_OPS); // 留给 worker panic 与通道断开
    fs::write(dir.0.join("y.log"), b"v").unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "worker 崩溃后引擎未在 5s 内退出（flush 失败未上抛？）"
        );
        sleep(Duration::from_millis(50));
    }
    let res = handle.join().expect("引擎线程 join");
    let msg = res.expect_err("worker 崩溃应使引擎以 Err 退出");
    assert!(msg.contains("worker"), "错误应指向动作 worker，实际: {msg}");
    stop.store(true, Ordering::Release);
}

/// 场景 12：监听路径不存在时真实二进制 exit 2（FR-01 canonicalize 失败的端到端锚定）。
#[test]
fn s12_nonexistent_root_exits_2() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("/nonexistent-fnotify-it-xyz")
        .output()
        .expect("启动 fnotify 二进制");
    assert_eq!(
        out.status.code(),
        Some(2),
        "路径不存在应 exit 2，实际 {:?}",
        out.status
    );
    assert!(!out.stderr.is_empty(), "stderr 应有错误说明");
}

/// 场景 13：--flush immediate——每个（路径,事件名）首事件立即交付，
/// 一个去抖窗口内同键重复被抑制；窗口期满后可再次交付。
#[test]
fn s13_immediate_flush_suppresses_repeats() {
    let dir = scratch("s13");
    let f = dir.0.join("hot.log");
    fs::write(&f, b"seed").unwrap();
    let e = start(opts_imm(&dir.0, 100)); // 100ms 抑制窗口
    sleep(START_SETTLE);

    // 100ms 窗口内连写 3 次：leading-edge 只交付首个 modify，后续同键被抑制。
    for i in 0..3 {
        fs::write(&f, format!("v{i}")).unwrap();
        sleep(Duration::from_millis(20));
    }
    let got = collect_events(&e, "hot.log", Duration::from_secs(2));
    let modifies = got.iter().filter(|it| it.event == "modify").count();
    assert_eq!(
        modifies, 1,
        "100ms 窗口内连写应抑制为 1 行 modify，实际 {modifies}"
    );

    // 窗口期满后再写：同键恢复交付。
    sleep(Duration::from_millis(300));
    fs::write(&f, b"later").unwrap();
    expect_event(&e, "modify", "hot.log");
}

/// 场景 14（回归）：immediate 模式下写间隔持续小于窗口时 poll_event 永不超时，
/// 若事件分支不查停机标志，shutdown 会被繁忙流无限拖住（s5 的 immediate 版）。
/// 断言：预置 shutdown 后引擎在有界时间内退出，且期间已有交付。
#[test]
fn s14_immediate_busy_stream_shutdown_is_bounded() {
    let dir = scratch("s14");
    let f = dir.0.join("busy.log");
    fs::write(&f, b"seed").unwrap();
    let mut e = start(opts_imm(&dir.0, 100)); // 100ms 抑制窗口
    sleep(START_SETTLE);

    // 写手线程：10ms 间隔连写（<< 100ms 窗口），10s 硬上限防止线程泄漏。
    let writer_stop = Arc::new(AtomicBool::new(false));
    let wflag = writer_stop.clone();
    let wpath = f.clone();
    let writer = std::thread::spawn(move || {
        let hard_stop = Instant::now() + Duration::from_secs(10);
        for i in 0.. {
            if wflag.load(Ordering::Relaxed) || Instant::now() >= hard_stop {
                break;
            }
            let _ = fs::write(&wpath, format!("v{i}"));
            sleep(Duration::from_millis(10));
        }
    });

    // 等到至少一次交付（immediate 随达随交，应很快），随后预置 shutdown。
    let mut early = 0usize;
    let first_deadline = Instant::now() + WAIT;
    while early == 0 {
        while let Ok(it) = e.rx.try_recv() {
            if file_name_of(&it.path) == "busy.log" {
                early += 1;
            }
        }
        assert!(
            Instant::now() < first_deadline,
            "繁忙流跑了 {WAIT:?} 仍无任何交付（immediate 未随达随交？）"
        );
        sleep(Duration::from_millis(20));
    }
    e.stop.store(true, Ordering::Release); // 预置 shutdown

    let handle = e.handle.take().expect("引擎线程句柄");
    let deadline = Instant::now() + Duration::from_secs(3); // 有界退出
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "immediate 繁忙流下引擎未在 3s 内退出（事件分支漏查停机？）"
        );
        sleep(Duration::from_millis(20));
    }
    let _ = handle.join();
    writer_stop.store(true, Ordering::Relaxed);
    let _ = writer.join();
}

/// 场景 15：多监听目录的逐目录摘除——删掉一个根只摘除它、其余目录继续监听；
/// 全部删光才优雅退出（引擎返回 Ok，不经错误通道）。
#[test]
fn s15_per_root_removal_keeps_others_watched() {
    let dir_a = scratch("s15a");
    let dir_b = scratch("s15b");
    let o = Options {
        paths: vec![dir_a.0.clone(), dir_b.0.clone()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        oneshot: false,
        until_idle: None,
        ready_file: None,
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms: 50,
        flush_immediate: false,
    };
    let mut e = start(o);
    sleep(START_SETTLE);

    // 删掉根 A：引擎应继续运行，B 目录事件照常交付。
    fs::remove_dir_all(&dir_a.0).unwrap();
    sleep(BETWEEN_OPS);
    fs::write(dir_b.0.join("keep.log"), b"x").unwrap();
    expect_event(&e, "create", "keep.log");

    // 删掉根 B（最后一个）：引擎应优雅退出。预算放宽到 10s（高负载主机
    // 下同 s6 的理由）。
    fs::remove_dir_all(&dir_b.0).unwrap();
    let handle = e.handle.take().expect("引擎线程句柄");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "全部监听目录删除后引擎未在 10s 内退出"
        );
        sleep(Duration::from_millis(50));
    }
    let _ = handle.join();
    // 逐目录摘除是正常路径：不得有引擎错误上报。
    assert!(e.err_rx.try_recv().is_err(), "逐目录摘除不得产生引擎错误");
}

/// 场景 16：metadata 事件细分——IN_ATTRIB（chmod/touch -m）独立为 metadata 事件，
/// 不再并入 modify；默认事件集不含 metadata。
#[test]
fn s16_metadata_event_split_from_attrib() {
    use std::os::unix::fs::PermissionsExt;

    let dir = scratch("s16");
    let f = dir.0.join("m.log");
    fs::write(&f, b"x").unwrap();

    // 默认事件集：chmod 应被过滤。屏障法：先 chmod（应过滤），再写内容
    // （默认集必报 modify）；收集全程，断言无 metadata 且屏障 modify 到达。
    let e = start(opts(&dir.0, false, vec![], 50));
    sleep(START_SETTLE);
    fs::set_permissions(&f, fs::Permissions::from_mode(0o600)).unwrap();
    sleep(BETWEEN_OPS);
    fs::write(&f, b"v2").unwrap();
    let got = collect_events(&e, "m.log", Duration::from_secs(2));
    assert!(
        got.iter().all(|it| it.event != "metadata"),
        "默认事件集不应投递 metadata，实际 {got:?}"
    );
    assert!(
        got.iter().any(|it| it.event == "modify"),
        "屏障失效：默认集未报写入的 modify"
    );
    drop(e);

    // 订阅 metadata,modify：chmod 报 metadata，写内容仍报 modify（两类互不替代）。
    let mask = engine::BIT_METADATA | engine::BIT_MODIFY;
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        oneshot: false,
        until_idle: None,
        ready_file: None,
        event_mask: mask,
        debounce_ms: 50,
        flush_immediate: false,
    };
    let e = start(o);
    sleep(START_SETTLE);
    fs::set_permissions(&f, fs::Permissions::from_mode(0o644)).unwrap();
    expect_event(&e, "metadata", "m.log");
    sleep(BETWEEN_OPS);
    fs::write(&f, b"v3").unwrap();
    expect_event(&e, "modify", "m.log");
}

/// 场景 17：监听目录被 mv 出监听树（IN_MOVE_SELF 探测）——按删除同语义
/// 逐目录摘除：引擎继续监听其余目录；旧路径重建的同名目录不被接管。
#[test]
fn s17_root_moved_out_is_dropped() {
    let dir_a = scratch("s17a");
    let dir_b = scratch("s17b");
    let outside = std::env::temp_dir().join(format!("fnotify-it-s17-out-{}", std::process::id()));
    let _ = fs::remove_dir_all(&outside);
    let o = Options {
        paths: vec![dir_a.0.clone(), dir_b.0.clone()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        oneshot: false,
        until_idle: None,
        ready_file: None,
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms: 50,
        flush_immediate: false,
    };
    let e = start(o);
    sleep(START_SETTLE);

    // 根 A 被 mv 走：摘除 A，B 照常。
    fs::rename(&dir_a.0, &outside).unwrap();
    sleep(BETWEEN_OPS);
    fs::write(dir_b.0.join("keep.log"), b"x").unwrap();
    expect_event(&e, "create", "keep.log");

    // 旧路径重建同名目录：其中活动不得上报。屏障法：先写 ghost.log（应过滤），
    // 再写 B 的屏障文件（必报）；屏障到达即 ghost 的投递窗口已过，逐条断言。
    fs::create_dir(&dir_a.0).unwrap();
    fs::write(dir_a.0.join("ghost.log"), b"x").unwrap();
    sleep(BETWEEN_OPS);
    fs::write(dir_b.0.join("barrier.log"), b"x").unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        assert!(
            remain > Duration::ZERO,
            "{WAIT:?} 内未等到 barrier.log 的 create"
        );
        match e.rx.recv_timeout(remain) {
            Ok(it) => {
                assert_ne!(
                    file_name_of(&it.path),
                    "ghost.log",
                    "已摘除根的旧路径上不得有事件上报"
                );
                if it.event == "create" && file_name_of(&it.path) == "barrier.log" {
                    break;
                }
            }
            Err(_) => panic!("引擎提前退出{}", engine_err_hint(&e)),
        }
    }

    // 摘掉最后一个根 B：引擎优雅退出。
    fs::remove_dir_all(&dir_b.0).unwrap();
    drop(e); // Engine Drop 停线程；摘除退出已由 s15 覆盖，此处只验证存活期语义
    let _ = fs::remove_dir_all(&outside);
}

/// 场景 18：exec 并发池与超时。
/// 并发：--jobs 2 下同批两个事件的 exec 同时在跑（第二个 start 先于第一个 end 落盘；
/// 串行实现会是 start,end,start,end）。超时：--timeout 1 杀掉 sleep 30，停机不被拖死。
#[test]
fn s18_exec_pool_concurrency_and_timeout() {
    let dir = scratch("s18");
    let out = std::env::temp_dir().join(format!("fnotify-it-s18-{}.log", std::process::id()));
    let _ = fs::remove_file(&out);
    let tpl = format!(
        "echo %e-start >> '{}'; sleep 0.5; echo %e-end >> '{}'",
        out.display(),
        out.display()
    );
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-q")
        .arg("--jobs")
        .arg("2")
        .arg("-x")
        .arg(&tpl)
        .arg(&dir.0)
        .spawn()
        .expect("启动 fnotify 二进制");
    sleep(START_SETTLE);
    fs::write(dir.0.join("c.log"), b"x").unwrap(); // 一批：create + modify 两条

    // 等两个 end 都落盘（并发 ~0.6s，给 4s 余量）。
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let ends = fs::read_to_string(&out)
            .map(|s| s.lines().filter(|l| l.ends_with("-end")).count())
            .unwrap_or(0);
        if ends >= 2 {
            break;
        }
        assert!(Instant::now() < deadline, "4s 内两个 exec 未都结束");
        sleep(Duration::from_millis(50));
    }
    let content = fs::read_to_string(&out).expect("读结果文件");
    let lines: Vec<&str> = content.lines().collect();
    let first_end = lines
        .iter()
        .position(|l| l.ends_with("-end"))
        .expect("必有 end 行");
    let second_start = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.ends_with("-start"))
        .nth(1)
        .map(|(i, _)| i)
        .expect("必有第二个 start 行");
    assert!(
        second_start < first_end,
        "--jobs 2 下两个 exec 应并发：第二个 start 应先于第一个 end，实际 {lines:?}"
    );
    // 优雅退出（在跑进程已空，应立即）。
    graceful_exit(child, "并发池 fnotify");
    let _ = fs::remove_file(&out);

    // 超时：--timeout 1 下 sleep 30 应被杀；SIGINT 后整体退出远快于 30s。
    let dir2 = scratch("s18b");
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-q")
        .arg("--jobs")
        .arg("1")
        .arg("--timeout")
        .arg("1")
        .arg("-x")
        .arg("sleep 30")
        .arg(&dir2.0)
        .spawn()
        .expect("启动 fnotify 二进制");
    sleep(START_SETTLE);
    fs::write(dir2.0.join("t.log"), b"x").unwrap();
    sleep(Duration::from_millis(300)); // 等 exec 启动
    graceful_exit(child, "超时池 fnotify");
}

/// 发 SIGINT 并等待子进程退出（限 5s），断言 exit 0。
fn graceful_exit(mut child: std::process::Child, who: &str) {
    let pid = child.id().to_string();
    std::process::Command::new("kill")
        .arg("-INT")
        .arg(&pid)
        .status()
        .expect("发送 SIGINT");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                assert!(status.success(), "{who} 应优雅退出 exit 0，实际 {status}");
                return;
            }
            None => {
                assert!(
                    Instant::now() < deadline,
                    "{who} 未在 5s 内退出（exec 池拖住停机？）"
                );
                sleep(Duration::from_millis(50));
            }
        }
    }
}

/// 场景 19：webhook——每个去抖批次以 JSON 数组 POST 到 --webhook 地址
///（http:// 明文；元素与 JSONL 行同构）。起一个一次性 HTTP 服务器验证。
#[test]
fn s19_webhook_posts_json_array() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let dir = scratch("s19");
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定临时端口");
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel::<String>();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().expect("接受连接");
        let mut got = Vec::new();
        let mut buf = [0u8; 4096];
        // 循环读到 headers 完整且 body 达到 Content-Length（TCP 不保证一次读全）。
        let text = loop {
            let s = String::from_utf8_lossy(&got).into_owned();
            if let Some((head, body)) = s.split_once("\r\n\r\n") {
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().to_string())
                    })
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                if body.len() >= len {
                    break s;
                }
            }
            let n = sock.read(&mut buf).expect("读请求");
            assert!(n > 0, "连接意外关闭");
            got.extend_from_slice(&buf[..n]);
        };
        assert!(text.starts_with("POST /hook "), "请求行异常: {text}");
        assert!(
            text.to_lowercase()
                .contains("content-type: application/json"),
            "Content-Type 异常: {text}"
        );
        let body = text
            .split("\r\n\r\n")
            .nth(1)
            .unwrap_or_default()
            .to_string();
        let _ = tx.send(body);
        let _ = sock.write_all(
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
    });

    let child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-q")
        .arg("--webhook")
        .arg(format!("http://127.0.0.1:{port}/hook"))
        .arg(&dir.0)
        .spawn()
        .expect("启动 fnotify 二进制");
    sleep(START_SETTLE);
    fs::write(dir.0.join("w.log"), b"x").unwrap(); // 一批：create + modify

    let body = rx
        .recv_timeout(Duration::from_secs(3))
        .expect("3s 内 webhook 应收到 POST");
    assert!(
        body.starts_with('[') && body.ends_with(']'),
        "请求体应为 JSON 数组，实际 {body}"
    );
    assert!(
        body.contains("\"event\":\"create\"") && body.contains("w.log"),
        "请求体应含事件内容，实际 {body}"
    );
    server.join().expect("HTTP 服务器线程");
    graceful_exit(child, "webhook fnotify");
}

/// s20：--config 加载 TOML 配置，命令行 -g 替换配置里的 globs。
///
/// 配置设 globs = ["*.log"]，命令行 -g '*.conf' 应整体替换它：
/// w.log 被过滤、w.conf 通过。paths 由配置提供，命令行未给位置参数。
#[test]
fn s20_config_file_with_cli_override() {
    let dir = scratch("s20");
    let cfg = std::env::temp_dir().join(format!("fnotify-it-s20-{}.toml", std::process::id()));
    fs::write(
        &cfg,
        format!(
            "paths = [\"{}\"]\nglobs = [\"*.log\"]\ndebounce = 50\n",
            dir.0.display()
        ),
    )
    .unwrap();

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("--config")
        .arg(&cfg)
        .arg("-g")
        .arg("*.conf")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("启动 fnotify 二进制");
    let (tx, rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take().expect("取 stdout");
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(l) = line else { break };
            if tx.send(l).is_err() {
                break;
            }
        }
    });

    sleep(START_SETTLE);
    fs::write(dir.0.join("w.log"), b"x").unwrap(); // 配置里的 *.log 已被命令行 -g 替换，应被过滤
    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("w.conf"), b"x").unwrap(); // 命令行 glob，应通过

    let deadline = Instant::now() + WAIT;
    let mut lines: Vec<String> = Vec::new();
    let mut saw_conf = false;
    while Instant::now() < deadline && !saw_conf {
        if let Ok(l) = rx.recv_timeout(Duration::from_millis(100)) {
            saw_conf = l.contains("w.conf");
            lines.push(l);
        }
    }
    assert!(saw_conf, "w.conf 应经命令行 glob 送达，实际 {lines:?}");
    // 屏障：w.log 先于 w.conf 创建；等到 w.conf 后再确认 w.log 缺席。
    sleep(Duration::from_millis(200));
    while let Ok(l) = rx.try_recv() {
        lines.push(l);
    }
    assert!(
        !lines.iter().any(|l| l.contains("w.log")),
        "w.log 应被过滤，实际 {lines:?}"
    );

    graceful_exit(child, "config fnotify");
    reader.join().expect("stdout reader");
    let _ = fs::remove_file(&cfg);
}

/// s21：exec 生命周期记录进 stdout 结构化流（type 判别，start/end 以 pid 关联）。
///
/// `-x 'true'` 未 -q：每个事件除事件行外还应产出 exec_start + exec_end 两行，
/// exec_end 带退出码与时长；事件行带 type 判别字段、ts 为 RFC3339。
#[test]
fn s21_exec_lifecycle_records_on_stdout() {
    let dir = scratch("s21");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-x")
        .arg("true")
        .arg(&dir.0)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("启动 fnotify 二进制");
    let (tx, rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take().expect("取 stdout");
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(l) = line else { break };
            if tx.send(l).is_err() {
                break;
            }
        }
    });

    sleep(START_SETTLE);
    fs::write(dir.0.join("w.log"), b"x").unwrap(); // create + modify → 两次 exec

    let deadline = Instant::now() + WAIT;
    let mut events: Vec<String> = Vec::new();
    let mut starts: Vec<String> = Vec::new();
    let mut ends: Vec<String> = Vec::new();
    while Instant::now() < deadline && ends.len() < 2 {
        if let Ok(l) = rx.recv_timeout(Duration::from_millis(100)) {
            if l.contains("\"type\":\"event\"") {
                events.push(l);
            } else if l.contains("\"type\":\"exec_start\"") {
                starts.push(l);
            } else if l.contains("\"type\":\"exec_end\"") {
                ends.push(l);
            }
        }
    }
    assert!(!events.is_empty(), "应有事件行，实际 {starts:?} {ends:?}");
    assert!(
        events.iter().any(|l| l.contains("Z\",\"event\"")),
        "事件行 ts 应为 RFC3339 UTC（…Z 收尾），实际 {events:?}"
    );
    assert_eq!(
        starts.len(),
        ends.len(),
        "start/end 应成对出现: {starts:?} vs {ends:?}"
    );
    assert!(!starts.is_empty(), "应有 exec_start 记录");

    // start/end 以 pid 关联。
    let start_pid = starts[0]
        .split("\"pid\":")
        .nth(1)
        .and_then(|s| s.split(',').next())
        .expect("exec_start 应含 pid");
    assert!(
        ends.iter()
            .any(|e| e.contains(&format!("\"pid\":{start_pid},"))),
        "exec_end 应以同 pid 关联（pid={start_pid}）: {ends:?}"
    );
    // 正常退出形态：exit=0、signal=null、timed_out=false、时长字段在场。
    assert!(
        ends.iter().any(|e| {
            e.contains("\"exit\":0")
                && e.contains("\"signal\":null")
                && e.contains("\"timed_out\":false")
                && e.contains("\"duration_ms\":")
        }),
        "应有正常退出的 exec_end: {ends:?}"
    );

    graceful_exit(child, "lifecycle fnotify");
    reader.join().expect("stdout reader");
}

/// s22：close_write 慢写者论证。
///
/// 掩码层 opt-in：`-e modify,close_write` 下，慢写者（写 → sleep ≥6×去抖窗口 →
/// 写 → 关闭）产出 ≥2 个 modify 批 + 恰好 1 条 close_write；同一操作序列在默认
/// 事件集下 close_write 为 0（对照运行）。写入用显式 open/write/close 模拟
/// cp/scp/rsync 之外的普通写路径，close 由 drop 触发。
#[test]
fn s22_close_write_slow_writer() {
    use std::io::Write as _;

    // 慢写：两段写入间隔 700ms（窗口 100ms 的 7 倍），必落在不同批次。
    let slow_write = |path: &Path| {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .expect("打开慢写文件");
        f.write_all(b"part1").unwrap();
        sleep(Duration::from_millis(700));
        f.write_all(b"part2").unwrap();
        drop(f); // close → IN_CLOSE_WRITE
    };

    // 主运行：订阅 modify + close_write。
    let dir = scratch("s22-main");
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        event_mask: engine::BIT_MODIFY | engine::BIT_CLOSE_WRITE,
        debounce_ms: 100,
        flush_immediate: false,
        oneshot: false,
        until_idle: None,
        ready_file: None,
    };
    let e = start(o);
    sleep(START_SETTLE);
    slow_write(&dir.0.join("slow.bin"));
    let got = collect_events(&e, "slow.bin", Duration::from_millis(1500));
    let modifies = got.iter().filter(|it| it.event == "modify").count();
    let closes = got.iter().filter(|it| it.event == "close_write").count();
    assert!(
        modifies >= 2,
        "慢写者应产出 ≥2 个 modify 批，实际 {modifies} 条: {got:?}"
    );
    assert_eq!(
        closes, 1,
        "慢写者应恰好产出 1 条 close_write，实际 {closes} 条: {got:?}"
    );

    // 对照运行：默认事件集，同一操作序列，close_write 必须为 0。
    let dir2 = scratch("s22-ctl");
    let e2 = start(opts(&dir2.0, false, vec![], 100));
    sleep(START_SETTLE);
    slow_write(&dir2.0.join("slow.bin"));
    let got2 = collect_events(&e2, "slow.bin", Duration::from_millis(1500));
    assert!(
        got2.iter().all(|it| it.event != "close_write"),
        "默认事件集不得产出 close_write: {got2:?}"
    );
}

/// s23：--exclude 反向 glob 集成（与 -g 同给，exclude 优先）。
///
/// 屏障法负断言：skip-a.log 同时命中 *.log 与 skip-*，应被排除；
/// 等到 b.log 屏障事件后断言 skip-a.log 全程缺席。
#[test]
fn s23_exclude_wins_over_glob() {
    let dir = scratch("s23");
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec!["*.log".into()],
        excludes: vec!["skip-*".into()],
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms: 50,
        flush_immediate: false,
        oneshot: false,
        until_idle: None,
        ready_file: None,
    };
    let e = start(o);
    sleep(START_SETTLE);

    fs::write(dir.0.join("a.log"), b"x").unwrap();
    expect_event(&e, "create", "a.log"); // -g 命中、exclude 未命中 → 通过

    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("skip-a.log"), b"x").unwrap(); // 两者皆命中 → 排除
    sleep(BETWEEN_OPS);
    fs::write(dir.0.join("b.log"), b"x").unwrap();

    // 屏障：等到 b.log 的 create；期间任何 skip-a.log 事件都直接判失败。
    let deadline = Instant::now() + WAIT;
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            panic!(
                "{WAIT:?} 内未等到 b.log 的 create 事件{}",
                engine_err_hint(&e)
            );
        }
        match e.rx.recv_timeout(remain) {
            Ok(it) => {
                let name = file_name_of(&it.path);
                assert_ne!(
                    name, "skip-a.log",
                    "skip-a.log 应被 --exclude 排除（优先于 -g），却收到 {}",
                    it.event
                );
                if name == "b.log" && it.event == "create" {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "{WAIT:?} 内未等到 b.log 的 create 事件{}",
                    engine_err_hint(&e)
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("引擎提前退出{}", engine_err_hint(&e))
            }
        }
    }
}

/// s24/s25 共用：引擎自行退出的运行支架（oneshot / until-idle 不走信号）。
/// 返回（事件接收端, 完成接收端, 停机标志）：完成通道收到引擎返回值即自行退出。
fn start_self_exit(
    o: Options,
) -> (
    mpsc::Receiver<BatchItem>,
    mpsc::Receiver<Result<(), String>>,
    Arc<AtomicBool>,
) {
    let (tx, rx) = mpsc::channel::<BatchItem>();
    let (dtx, drx) = mpsc::channel::<Result<(), String>>();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    std::thread::spawn(move || {
        let sink: engine::Sink = Box::new(move |batch: &[BatchItem]| {
            for item in batch {
                let _ = tx.send(item.clone());
            }
        });
        let _ = dtx.send(engine::run_with_shutdown(o, sink, flag));
    });
    (rx, drx, stop)
}

/// s24：--oneshot 无信号有界退出。
///
/// 全部被过滤的事件不触发退出（负断言）；首个通过的批次交付后引擎走优雅停机，
/// 无需信号、exit 0（Result 为 Ok）。
#[test]
fn s24_oneshot_exits_after_first_delivered_batch() {
    let dir = scratch("s24");
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec!["*.done".into()],
        excludes: vec![],
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms: 50,
        flush_immediate: false,
        oneshot: true,
        until_idle: None,
        ready_file: None,
    };
    let (rx, done, stop) = start_self_exit(o);
    sleep(START_SETTLE);

    // 负断言：被过滤的事件不触发 oneshot。
    fs::write(dir.0.join("noise.txt"), b"x").unwrap();
    sleep(Duration::from_millis(800));
    assert!(
        done.try_recv().is_err(),
        "全部被过滤时不得退出（oneshot 判的是通过过滤的批次）"
    );

    // 首个通过的批次：交付后自行退出。
    fs::write(dir.0.join("a.done"), b"x").unwrap();
    let first = rx
        .recv_timeout(WAIT)
        .expect("oneshot 运行应交付至少一个批次");
    assert_eq!(file_name_of(&first.path), "a.done");
    let r = done
        .recv_timeout(Duration::from_secs(3))
        .expect("oneshot 应在交付首批后自行退出（无需信号）");
    assert!(r.is_ok(), "oneshot 退出应优雅（Ok），实际 {r:?}");
    stop.store(true, Ordering::Release); // 已退出，无害
}

/// s25：--until-idle 时序——持续事件期间不退出，静默满 SEC 秒自行退出。
#[test]
fn s25_until_idle_exits_after_quiet_period() {
    let dir = scratch("s25");
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms: 50,
        flush_immediate: false,
        oneshot: false,
        until_idle: Some(Duration::from_secs(1)),
        ready_file: None,
    };
    let (_rx, done, stop) = start_self_exit(o);
    sleep(START_SETTLE);

    // 持续事件（每 200ms 一个文件，共约 2s）：每次交付都重置计时，不得退出。
    for i in 0..8 {
        fs::write(dir.0.join(format!("f{i}.txt")), b"x").unwrap();
        sleep(Duration::from_millis(200));
        assert!(
            done.try_recv().is_err(),
            "持续事件期间 until-idle 不得退出（第 {i} 轮）"
        );
    }
    // 停止制造事件：静默满 1s 后自行退出。
    let r = done
        .recv_timeout(Duration::from_secs(4))
        .expect("静默满 1s 后 until-idle 应自行退出");
    assert!(r.is_ok(), "until-idle 退出应优雅（Ok），实际 {r:?}");
    stop.store(true, Ordering::Release); // 已退出，无害
}

/// s26：--oneshot 在**持续写入流**下仍能自行退出。
///
/// 修复前收尾 drain 用 poll_event(ZERO) 无界读——持续写入（tail -f 形态）下
/// 永远读得到新事件，oneshot 命中后挂在收尾不退出（信号路径有二次 Ctrl-C
/// 兜底，oneshot 没有）。修复后收尾走 drain_bounded（总时长封顶 2×window）。
#[test]
fn s26_oneshot_exits_under_continuous_writes() {
    let dir = scratch("s26");
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec!["*.done".into()],
        excludes: vec![],
        event_mask: engine::DEFAULT_EVENTS,
        debounce_ms: 50,
        flush_immediate: false,
        oneshot: true,
        until_idle: None,
        ready_file: None,
    };
    let (_rx, done, stop) = start_self_exit(o);
    sleep(START_SETTLE);

    // tail -f 形态的后台写入：无间隔紧密写、持续 8s——**贯穿整个断言窗口**。
    // 这是本测试的回归意义所在：writer 一旦有停歇或写入间隔，旧实现的
    // poll_event(ZERO) 非阻塞收尾就可能在某个空档探测到空队列而"恰好"
    // 退出，测试便锁不住旧 bug（验收复核抓到的空窗）；紧密流下旧实现的
    // 收尾被新事件无限续命，done 永远不会来。
    let stream = dir.0.join("stream.log");
    fs::write(&stream, b"").unwrap();
    let stop_w = Arc::new(AtomicBool::new(false));
    let stop_wc = stop_w.clone();
    let writer = std::thread::spawn(move || {
        use std::io::Write;
        let Ok(mut fh) = fs::OpenOptions::new().append(true).open(&stream) else {
            return;
        };
        let start = Instant::now();
        while !stop_wc.load(Ordering::Acquire) && start.elapsed() < Duration::from_secs(8) {
            let _ = writeln!(fh, "line");
        }
    });

    // 触发 oneshot：交付后必须在 writer 仍在紧密写入时就完成收尾退出——
    // 旧实现下此刻 done 永远不会来（drain 被新事件无限续命）。
    fs::write(dir.0.join("a.done"), b"x").unwrap();
    let r = done
        .recv_timeout(Duration::from_secs(5))
        .expect("持续写入流下 oneshot 仍应在有界时间内自行退出");
    assert!(r.is_ok(), "退出应优雅（Ok），实际 {r:?}");
    stop.store(true, Ordering::Release); // 已退出，无害
    stop_w.store(true, Ordering::Release);
    writer.join().unwrap();
}

/// s27：递归监听遇 EACCES 子目录——告警一行到 stderr、其余子树照常监听。
/// 修复前 collect_dirs 静默跳过：用户以为全覆盖实际缺一块。
/// uid=0 绕过 chmod 000，权限语义不可测（root 守卫惯例，同 suoji difftest）。
#[test]
fn s27_unreadable_subtree_warns_and_continues() {
    if unsafe { libc::getuid() } == 0 {
        eprintln!("SKIP s27 (uid=0: root 绕过 chmod 000，EACCES 不可造)");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch("s27");
    fs::create_dir_all(dir.0.join("ok")).unwrap();
    fs::write(dir.0.join("ok/visible.txt"), b"x").unwrap();
    fs::create_dir_all(dir.0.join("noperm/inner")).unwrap();
    fs::write(dir.0.join("noperm/inner/hidden.txt"), b"x").unwrap();
    let mut pm = fs::metadata(dir.0.join("noperm")).unwrap().permissions();
    pm.set_mode(0o000);
    fs::set_permissions(dir.0.join("noperm"), pm).unwrap();

    // --oneshot：可见文件触发交付后自行退出。stdout/stderr 显式管道化
    // （spawn 默认 inherit，wait_with_output 收不到继承流）。
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env_remove("FNOTIFY_LANG")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .arg("--oneshot")
        .arg("-r")
        .arg(&dir.0)
        .spawn()
        .expect("启动 fnotify 二进制");
    sleep(START_SETTLE);
    fs::write(dir.0.join("trigger.txt"), b"x").unwrap();
    let out = child.wait_with_output().expect("wait fnotify");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cannot scan") && stderr.contains("noperm"),
        "EACCES 子目录应告警一行（含目录名），实际 status={:?} stdout={:?} stderr: {stderr:?}",
        out.status,
        String::from_utf8_lossy(&out.stdout)
    );
    // 可见子树照常监听（oneshot 被 trigger.txt 触发即证明主流程未受影响）
    assert!(
        out.status.success(),
        "应优雅退出 exit 0，实际 {:?}",
        out.status
    );
}

/// s28（回归）：创建风暴下的接管完整性——mv 入一棵大树拉长接管窗口，
/// 窗口内持续在树内新建 late 目录；接管完成后逐文件读一遍，全部 late 文件
/// 的 access 必须出现在 JSONL（**零**缺失），抽样的大树深处文件也必须在场
/// （防"整体没接管"的假阳性对照）。二进制端到端：-r -e access、SIGTERM 收尾。
///
/// 修复前 takeover 是两阶段：先 DFS 快照全部现存子目录、再统一挂表。快照
/// readdir(D) 与 add(D) 之间的窗口内，D 下新建的目录既不在快照、CREATE 事件
/// 又因 D 的 watch 未挂而丢失，且无补偿 → 永久失聪（半聋，stderr 静默）。
/// 确定性放大器：万级目录的快照+挂表把窗口拉长到数十毫秒，late 目录创建期
/// （~0.6s，间隔 3ms 铺开）横跨窗口，缺失数目稳定可观。
#[test]
fn s28_takeover_storm_late_dirs_not_lost() {
    const TREE_DIRS: usize = 10000; // 大树规模：快照+挂表窗口实测数十毫秒
    const LATE_A: usize = 160; // late A：pkg 的直接子目录
    const LATE_B_EVERY: usize = 4; // 每 4 个 A 配 1 个 late B：大树 dNNNN 下的子目录
    const SAMPLES: usize = 20; // 大树抽样（快照必然覆盖，对照面）

    let base = scratch("s28");
    let root_raw = base.0.join("root");
    let stage = base.0.join("stage");
    fs::create_dir(&root_raw).unwrap();
    fs::create_dir(&stage).unwrap();
    // 输出路径与二进制同口径（canonicalize 后的绝对路径）。
    let root = fs::canonicalize(&root_raw).unwrap();

    // 预建大树（监听树外，不产生事件）：pkg/dNNNN × TREE_DIRS，每目录一个文件。
    let pkg = stage.join("pkg");
    fs::create_dir_all(&pkg).unwrap();
    for i in 0..TREE_DIRS {
        let d = pkg.join(format!("d{i:05}"));
        fs::create_dir(&d).unwrap();
        fs::write(d.join("f.log"), b"x").unwrap();
    }

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-r")
        .arg("-e")
        .arg("access")
        .arg("--debounce")
        .arg("50")
        .arg(&root)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("启动 fnotify 二进制");
    let (tx, rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take().expect("取 stdout");
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(l) = line else { break };
            if tx.send(l).is_err() {
                break;
            }
        }
    });

    sleep(START_SETTLE);

    // mv 入监听树：MOVED_TO|ISDIR 触发接管，快照必然覆盖整棵大树 → 窗口被拉长。
    fs::rename(&pkg, root.join("pkg")).unwrap();

    // mv 后立即持续创建 late 目录（创建期横跨接管窗口），每个放一个文件：
    // A = pkg 直接子目录（窗口 = readdir(pkg) 枚举过位之后到 add(pkg)）；
    // B = 大树 dNNNN 下的子目录（窗口 = 父目录枚举之后到 add(父)，含整个挂表阶段）。
    let mut late_files: Vec<PathBuf> = Vec::new();
    for i in 0..LATE_A {
        let d = root.join("pkg").join(format!("lateA{i:04}"));
        fs::create_dir(&d).unwrap();
        fs::write(d.join("f.log"), b"x").unwrap();
        late_files.push(d.join("f.log"));
        if i % LATE_B_EVERY == 0 {
            let parent = root
                .join("pkg")
                .join(format!("d{:05}", (i * 7919) % TREE_DIRS));
            let d = parent.join(format!("lateB{i:04}"));
            fs::create_dir(&d).unwrap();
            fs::write(d.join("f.log"), b"x").unwrap();
            late_files.push(d.join("f.log"));
        }
        sleep(Duration::from_millis(3));
    }

    // 等接管（含 late 目录经 CREATE 事件的补接管）彻底完成，留足余量。
    sleep(Duration::from_secs(2));

    // 访问阶段：抽样大树深处的文件（对照组）+ 读全部 late 文件。
    let mut sample_files: Vec<PathBuf> = Vec::new();
    for i in 0..SAMPLES {
        let f = root
            .join("pkg")
            .join(format!("d{:05}", i * (TREE_DIRS / SAMPLES)))
            .join("f.log");
        fs::read(&f).expect("读抽样文件");
        sample_files.push(f);
    }
    for f in &late_files {
        fs::read(f).expect("读 late 文件");
    }

    // 等投递（去抖 50ms，静默一个窗口即出批；1.5s 裕量充足）。
    sleep(Duration::from_millis(1500));

    // SIGTERM 优雅停机（drain 上限 2×window，事件早已读尽）。
    let mut stderr_handle = child.stderr.take();
    std::process::Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("发送 SIGTERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(st) => break st,
            None => {
                assert!(Instant::now() < deadline, "SIGTERM 后 5s 内 fnotify 未退出");
                sleep(Duration::from_millis(50));
            }
        }
    };
    let mut err_bytes = Vec::new();
    if let Some(mut h) = stderr_handle.take() {
        use std::io::Read;
        let _ = h.read_to_end(&mut err_bytes);
    }
    let stderr = String::from_utf8_lossy(&err_bytes).into_owned();
    reader.join().expect("stdout reader");
    let mut lines: Vec<String> = Vec::new();
    while let Ok(l) = rx.try_recv() {
        lines.push(l);
    }

    assert!(
        status.success(),
        "SIGTERM 应优雅退出 exit 0，实际 {status}，stderr: {stderr:?}"
    );
    let access_hit = |lines: &[String], p: &Path| {
        let ps = p.display().to_string();
        lines
            .iter()
            .any(|l| l.contains("\"event\":\"access\"") && l.contains(&ps))
    };
    let sample_missing: Vec<String> = sample_files
        .iter()
        .filter(|f| !access_hit(&lines, f))
        .map(|f| f.display().to_string())
        .collect();
    assert!(
        sample_missing.is_empty(),
        "抽样的大树文件必须全部在场（否则是整体没接管/管线断裂，非本回归目标），\
         缺 {sample_missing:?}，stdout 共 {} 行，stderr: {stderr:?}",
        lines.len()
    );
    let missing: Vec<String> = late_files
        .iter()
        .filter(|f| !access_hit(&lines, f))
        .map(|f| f.display().to_string())
        .collect();
    assert!(
        missing.is_empty(),
        "late 文件缺失 {} / {} 个（接管窗口内创建的目录永久失聪），前 10 个: {:?}，\
         stderr: {stderr:?}",
        missing.len(),
        late_files.len(),
        &missing[..missing.len().min(10)]
    );
}

/// s29：open 事件（IN_OPEN）opt-in 集成。
///
/// 订阅 open+access 后的三种访问形态：
/// a) 只 open 不读（File::open 后立即关闭）→ 仅 1 条 open，无 access；
/// b) read() 类读取（fs::read）→ open + access 双报（消费端按 path 去重前的
///    原始形态，双报是设计行为，不做事件合并）；
/// c) mmap 读（python3 mmap，mmap 缺页不走 read(2)）→ 有 open、无 access——
///    clang 等编译器经 mmap 读源码的整类访问只有 open 能覆盖，这是 open 准入的动机。
/// 对照运行：默认事件集不订阅 open，读文件不得产出任何 open 事件（掩码层 opt-in）。
#[test]
fn s29_open_event_opt_in() {
    // 主运行：订阅 open + access（等价 CLI -e access,open）。
    let dir = scratch("s29-main");
    fs::write(dir.0.join("a.bin"), b"x").unwrap(); // a：只开不读
    fs::write(dir.0.join("b.bin"), b"x").unwrap(); // b：read() 类读取
    fs::write(dir.0.join("c.bin"), b"x").unwrap(); // c：mmap 读
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        event_mask: engine::BIT_OPEN | engine::BIT_ACCESS,
        debounce_ms: 100,
        flush_immediate: false,
        oneshot: false,
        until_idle: None,
        ready_file: None,
    };
    let e = start(o);
    sleep(START_SETTLE);

    // a) 打开后立即关闭、不读：至少 1 条 open，且 access 不得多于 open。
    // 注：装有实时扫描类安全软件（EDR/杀软）的主机上，文件关闭后会被外部
    // 进程二次 open+read——那是真实文件活动，与本进程无关的额外 open/access
    // 对无法也不应被断言排除；可跨环境成立的不变式是 access 永远不多于
    // open（每次 access 必先有 open，本进程的不读 open 不产生 access）。
    drop(fs::File::open(dir.0.join("a.bin")).expect("打开 a.bin"));
    let got = collect_events(&e, "a.bin", Duration::from_millis(1500));
    let opens = got.iter().filter(|it| it.event == "open").count();
    let accesses = got.iter().filter(|it| it.event == "access").count();
    assert!(
        opens >= 1,
        "只开不读应至少产出 1 条 open，实际 0 条: {got:?}"
    );
    assert!(
        accesses <= opens,
        "access 不得多于 open（每次 access 必先有 open）: {got:?}"
    );

    // b) read() 类读取：open 与 access 各至少一条（双报是设计行为）。
    fs::read(dir.0.join("b.bin")).expect("读 b.bin");
    let got = collect_events(&e, "b.bin", Duration::from_millis(1500));
    assert!(
        got.iter().any(|it| it.event == "open") && got.iter().any(|it| it.event == "access"),
        "read() 类读取应产出 open 与 access 双报，实际: {got:?}"
    );

    // c) mmap 读：有 open、无 access（mmap 缺页不走 read(2)，IN_ACCESS 天然缺席）。
    // python3 缺失时跳过本臂而非 panic（a/b 臂不受影响；s27 的 uid 守卫同款策略）。
    let py_ok = std::process::Command::new("python3")
        .arg("-V")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|st| st.success())
        .unwrap_or(false);
    if !py_ok {
        eprintln!("s29: python3 不可用，跳过 mmap 臂（a/b 臂仍然有效）");
    } else {
        let out = std::process::Command::new("python3")
            .arg("-c")
            .arg(
                "import mmap;f=open('c.bin','rb');m=mmap.mmap(f.fileno(),0,\
                 access=mmap.ACCESS_READ);m[:1];m.close()",
            )
            .current_dir(&dir.0)
            .output()
            .expect("启动 python3 mmap 读");
        assert!(
            out.status.success(),
            "python3 mmap 读失败: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let got = collect_events(&e, "c.bin", Duration::from_millis(1500));
        let opens = got.iter().filter(|it| it.event == "open").count();
        let accesses = got.iter().filter(|it| it.event == "access").count();
        assert!(
            opens >= 1,
            "mmap 读应有 open 事件（open() 由 python 显式调用），实际: {got:?}"
        );
        // mmap 缺页不走 read(2)，本进程不产生 access；实时扫描类安全软件的
        // 二次 open+read 会成对带出 access（见 a 臂注释），故用
        // access < open 而非全否——本进程的 open 恰一条且不伴 access。
        assert!(
            accesses < opens,
            "mmap 读本进程不得产出 access（access 应少于 open）: {got:?}"
        );
    }

    // 对照运行：默认事件集，同一读操作，open 必须为 0（掩码层 opt-in）。
    let dir2 = scratch("s29-ctl");
    fs::write(dir2.0.join("b.bin"), b"x").unwrap();
    let e2 = start(opts(&dir2.0, false, vec![], 100));
    sleep(START_SETTLE);
    fs::read(dir2.0.join("b.bin")).expect("读对照 b.bin");
    let got2 = collect_events(&e2, "b.bin", Duration::from_millis(1500));
    assert!(
        got2.iter().all(|it| it.event != "open"),
        "默认事件集不得产出 open: {got2:?}"
    );
}

/// s30：--ready-file 启动就绪信号——多根（含递归子目录）全部挂表完成后才创建
/// 就绪文件；文件出现后立即写入的事件必须照常送达（"ready 后不丢事件"——
/// 消费端不再 sleep 赌挂表完成的全部依据）；SIGTERM 优雅排空退出后文件仍在
/// （不删除，生命周期归消费端，语义钉死）。
#[test]
fn s30_ready_file_appears_after_all_roots_watched() {
    let dir_a = scratch("s30a");
    let dir_b = scratch("s30b");
    fs::create_dir(dir_a.0.join("sub")).unwrap(); // 启动前已存在的递归子目录
    let ready = std::env::temp_dir().join(format!("fnotify-it-s30-ready-{}", std::process::id()));
    let _ = fs::remove_file(&ready);

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-r")
        .arg("--ready-file")
        .arg(&ready)
        .arg(&dir_a.0)
        .arg(&dir_b.0)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("启动 fnotify 二进制");
    let (tx, rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take().expect("取 stdout");
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(l) = line else { break };
            if tx.send(l).is_err() {
                break;
            }
        }
    });

    // 就绪文件应在合理超时内出现（小树挂表是毫秒级，5s 预算远超必要）。
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        assert!(
            Instant::now() < deadline,
            "5s 内就绪文件未出现（全部根挂表完成后应创建它）"
        );
        sleep(Duration::from_millis(50));
    }

    // 就绪文件出现即全部 watch（含递归子目录）已挂：此刻起的写入不得丢失。
    fs::write(dir_a.0.join("sub").join("deep.log"), b"x").unwrap();
    fs::write(dir_b.0.join("in-b.log"), b"x").unwrap();
    let deadline = Instant::now() + WAIT;
    let mut saw_deep = false;
    let mut saw_in_b = false;
    while !(saw_deep && saw_in_b) {
        let remain = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remain) {
            Ok(l) => {
                saw_deep |= l.contains("deep.log");
                saw_in_b |= l.contains("in-b.log");
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "{WAIT:?} 内未收齐两个根的事件（deep={saw_deep}, in-b={saw_in_b}），\
                     就绪后的事件不得丢失"
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("fnotify 提前退出")
            }
        }
    }

    // SIGTERM 优雅排空：exit 0，且就绪文件不被删除（消费端自己管理生命周期）。
    std::process::Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("发送 SIGTERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(st) => break st,
            None => {
                assert!(Instant::now() < deadline, "SIGTERM 后 5s 内 fnotify 未退出");
                sleep(Duration::from_millis(50));
            }
        }
    };
    assert!(status.success(), "SIGTERM 应优雅退出 exit 0，实际 {status}");
    assert!(ready.exists(), "SIGTERM 排空后就绪文件应仍在（不删除）");
    reader.join().expect("stdout reader");
    let _ = fs::remove_file(&ready);
}

/// s31：--ready-file 反例——启动失败（监听根不存在，exit 2）时绝不创建就绪文件：
/// "文件出现"才能安全地被消费端当作"监控已生效"的判据。
#[test]
fn s31_ready_file_not_created_when_startup_fails() {
    let ready = std::env::temp_dir().join(format!("fnotify-it-s31-ready-{}", std::process::id()));
    let _ = fs::remove_file(&ready);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("--ready-file")
        .arg(&ready)
        .arg("/nonexistent-fnotify-it-s31")
        .output()
        .expect("启动 fnotify 二进制");
    assert_eq!(
        out.status.code(),
        Some(2),
        "路径不存在应 exit 2，实际 {:?}",
        out.status
    );
    assert!(!ready.exists(), "启动失败不得创建就绪文件");
    assert!(!out.stderr.is_empty(), "stderr 应有错误说明");
}

/// 发 SIGTERM 并限时有界等待优雅退出；超时先 SIGKILL 收尸再 panic——
/// 楔死的引擎（dispatch 卡在批次内不回来）不能把测试进程挂死，且失败
/// 报告要能带上退出形态。
fn term_and_wait_bounded(child: &mut std::process::Child, who: &str) -> std::process::ExitStatus {
    let pid = child.id().to_string();
    std::process::Command::new("kill")
        .arg("-TERM")
        .arg(&pid)
        .status()
        .expect("发送 SIGTERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(st) => return st,
            None => {
                if Instant::now() >= deadline {
                    let _ = std::process::Command::new("kill")
                        .arg("-KILL")
                        .arg(&pid)
                        .status();
                    let st = child.wait().expect("SIGKILL 后收尸");
                    panic!(
                        "{who} 在 SIGTERM 后 5s 未退出（dispatch 楔死？），已 SIGKILL，退出形态 {st}"
                    );
                }
                sleep(Duration::from_millis(50));
            }
        }
    }
}

/// s32（回归）：风暴规模跨过内核队列上限的接管——20000 子目录一次 mv 入
/// 监听树。前序升级（父目录枚举完就恢复完整掩码、再下钻）会让下钻读子目录
/// 的 getdents 在**父目录**的 FULL 掩码 watch 上触发 IN_ACCESS（带子目录名）：
/// 2×N 自触发事件灌爆内核队列（默认配额 16384，20000 目录的 40000 事件必超）
/// → IN_Q_OVERFLOW → rescan 全树重扫 →
/// 再灌爆的自激循环，单线程 dispatch 楔在批次内（JSONL 0 输出）、SIGTERM
/// 挂死。万级以下规模自产事件数低于队列上限，s28 锁不到这一层——规模阈值
/// 问题。修复：后序升级（整棵子树 DFS 完成后才恢复完整掩码，下钻期间所有
/// 祖先保持扫描期掩码，自触发 ACCESS 落在摘除位上被内核抑制）。
/// 断言三面：全部文件 access 在场、stderr 无 overflow、SIGTERM 有界退出。
#[test]
fn s32_takeover_beyond_queue_limit_no_overflow() {
    const TREE_DIRS: usize = 20000; // 下钻自触发 ≈ 2×N；默认配额 16384 下必超

    let base = scratch("s32");
    let root_raw = base.0.join("root");
    let stage = base.0.join("stage");
    fs::create_dir(&root_raw).unwrap();
    fs::create_dir(&stage).unwrap();
    let root = fs::canonicalize(&root_raw).unwrap();

    // 预建大树（监听树外，不产生事件）：pkg/dNNNN × TREE_DIRS，每目录一个文件。
    let pkg = stage.join("pkg");
    fs::create_dir_all(&pkg).unwrap();
    for i in 0..TREE_DIRS {
        let d = pkg.join(format!("d{i:05}"));
        fs::create_dir(&d).unwrap();
        fs::write(d.join("f.log"), b"x").unwrap();
    }

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("-r")
        .arg("-e")
        .arg("access")
        .arg("--debounce")
        .arg("50")
        .arg(&root)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("启动 fnotify 二进制");
    let (tx, rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take().expect("取 stdout");
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(l) = line else { break };
            if tx.send(l).is_err() {
                break;
            }
        }
    });

    sleep(START_SETTLE);

    // mv 入监听树：MOVED_TO|ISDIR 触发接管，一次下钻跨过队列上限。
    fs::rename(&pkg, root.join("pkg")).unwrap();
    let new_pkg = root.join("pkg");
    let all_files: Vec<PathBuf> = (0..TREE_DIRS)
        .map(|i| new_pkg.join(format!("d{i:05}")).join("f.log"))
        .collect();
    sleep(Duration::from_secs(2)); // 等接管彻底完成（修复前此窗口内已开始自激）

    // 逐文件读全部大树文件（20k 次 fs::read；缺失即接管/投递断裂）。
    for f in &all_files {
        fs::read(f).expect("读大树文件");
    }
    sleep(Duration::from_millis(1500)); // 等投递

    let mut stderr_handle = child.stderr.take();
    let status = term_and_wait_bounded(&mut child, "风暴接管 fnotify");
    let mut err_bytes = Vec::new();
    if let Some(mut h) = stderr_handle.take() {
        use std::io::Read;
        let _ = h.read_to_end(&mut err_bytes);
    }
    let stderr = String::from_utf8_lossy(&err_bytes).into_owned();
    reader.join().expect("stdout reader");
    let mut lines: Vec<String> = Vec::new();
    while let Ok(l) = rx.try_recv() {
        lines.push(l);
    }

    assert!(
        status.success(),
        "SIGTERM 应优雅退出 exit 0，实际 {status}，stderr: {stderr:?}"
    );
    assert!(
        !stderr.contains("event queue overflow"),
        "接管不得触发事件队列溢出（扫描自触发应由扫描期掩码抑制），stderr: {stderr:?}"
    );
    // O(1) 查表：20k 文件 × 20k 行的逐条 contains 不可接受。
    let delivered: std::collections::HashSet<&str> = lines
        .iter()
        .filter(|l| l.contains("\"event\":\"access\""))
        .filter_map(|l| l.split("\"path\":\"").nth(1))
        .map(|rest| rest.split('"').next().unwrap_or(""))
        .collect();
    let missing: Vec<String> = all_files
        .iter()
        .filter(|f| !delivered.contains(f.display().to_string().as_str()))
        .map(|f| f.display().to_string())
        .collect();
    assert!(
        missing.is_empty(),
        "大树文件缺失 {} / {} 个（dispatch 楔死或接管丢失），前 10 个: {:?}，\
         stdout 共 {} 行，stderr: {:?}",
        missing.len(),
        all_files.len(),
        &missing[..missing.len().min(10)],
        lines.len(),
        stderr
    );
}

/// s33（回归）：-e open 下启动扫描零幻影——扫描自己的 opendir/getdents 与
/// ACCESS 同机制自触发 IN_OPEN（dir:true 的 open，实测一条起步）。扫描期
/// 掩码须同样摘除 OPEN，否则递归启动扫描 N 目录 = N 条幻影 open，万级树
/// 接管以 OPEN 洪流复现 s32 的自激死法。断言：就绪排空后**零** open 事件；
/// 随后打开一个已知文件恰好 1 条 open 且 dir=false（真实事件不受影响）。
#[test]
fn s33_open_scan_phantom_free_startup() {
    let dir = scratch("s33");
    // 启动前已存在的树：根 + 3 个子目录（各含文件），递归启动扫描必经。
    for i in 0..3 {
        let sub = dir.0.join(format!("sub{i}"));
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("f.log"), b"x").unwrap();
    }
    // 预沉降：实时扫描类安全软件（EDR/杀软）会对刚写的文件二次 open+read
    // （见 s29 注释），其事件若落在引擎启动之后会被误判为启动扫描幻影。
    // 先给扫描器一个完成窗口（其二次访问通常在 1s 内到达），再启动引擎。
    sleep(Duration::from_millis(1500));
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: true,
        globs: vec![],
        excludes: vec![],
        event_mask: engine::BIT_OPEN, // 只订阅 open（不带 access）
        debounce_ms: 50,
        flush_immediate: false,
        oneshot: false,
        until_idle: None,
        ready_file: None,
    };
    let e = start(o);
    sleep(START_SETTLE);

    // 屏障前：启动扫描的幻影 open（若有）在就绪排空窗口内必已送达。
    let mut phantoms: Vec<BatchItem> = Vec::new();
    while let Ok(it) = e.rx.try_recv() {
        phantoms.push(it);
    }
    assert!(
        phantoms.iter().all(|it| it.event != "open"),
        "启动扫描不得自触发 open 事件（幻影）: {phantoms:?}"
    );

    // 真实事件对照：打开已知文件（只开不读），至少 1 条 open、dir=false。
    // 实时扫描类安全软件可能对同一文件二次 open（见 s29 注释），故不断言
    // 恰好 1 条；扫描器不开目录，dir=false 对全部 open 成立。
    drop(fs::File::open(dir.0.join("sub0").join("f.log")).expect("打开已知文件"));
    let got = collect_events(&e, "f.log", Duration::from_millis(1500));
    let opens: Vec<&BatchItem> = got.iter().filter(|it| it.event == "open").collect();
    assert!(
        !opens.is_empty(),
        "打开一次应至少产出 1 条 open，实际 0 条: {got:?}"
    );
    assert!(
        opens.iter().all(|it| !it.dir),
        "文件的 open 事件 dir 应为 false: {got:?}"
    );
}

/// 给子进程发信号（s34 的 SIGSTOP/SIGCONT 放大器专用；SIGTERM 兜底走
/// term_and_wait_bounded 自带的 kill 命令路径）。
fn signal_pid(pid: u32, sig: &str) {
    std::process::Command::new("kill")
        .arg(format!("-{sig}"))
        .arg(pid.to_string())
        .status()
        .expect("发送信号");
}

/// s34（M2 降级可见性）：内核队列溢出不再静默——真实 IN_Q_OVERFLOW 时
/// stdout 必须输出 type:"overflow" 降级记录。放大器与 s32 同族但反向：
/// s32 断言修复后的接管不再触发溢出，本测试要的就是真实溢出——SIGSTOP
/// 冻结分发，监听根内预写足量文件（每文件 create+modify 两事件，总量按
/// 运行机的 max_queued_events 定标，1.25 倍确定性灌爆——不同主机的队列
/// 配额不同，硬编码 16384 会在大配额机器上永不溢出），SIGCONT 后引擎读到
/// 的批次里必有 Q_OVERFLOW（朴素 mkdir 风暴打不开窗口，见 design §3.4）。
/// 断言四面：
/// 1. stdout 出现 ≥1 条 {"type":"overflow"} 行，且该行不携带 path 字段
///    （type 判别，不进事件名字空间）；
/// 2. 默认订阅（不带 -e，掩码不含 other）下该行也在——溢出记录绕过订阅
///    掩码等聚合三道闸的实证；
/// 3. 溢出后新建文件的事件仍正常上报（rescan 重建 watch 恢复有效）；
/// 4. SIGTERM 有界优雅退出（exit 0）。
#[test]
fn s34_q_overflow_emits_degradation_record() {
    // 按运行机的内核队列配额定标：每文件 create+modify 两事件，文件数取
    // 配额的 5/8（事件量 1.25 倍）——进程冻结期间无消费，必爆。读不到配额
    // （非 Linux 等）时退回 16384 默认值。
    let queue_limit: usize = std::fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(16384);
    let flood_files: usize = queue_limit * 5 / 8; // ×2 事件 = 1.25× 配额

    let base = scratch("s34");
    let root_raw = base.0.join("root");
    fs::create_dir(&root_raw).unwrap();
    let root = fs::canonicalize(&root_raw).unwrap();

    // 不带 -e：默认订阅（create,modify,rename,remove）。若溢出记录走事件
    // 掩码闸（按未知名映射 other 位）会被默认掩码吞掉——它必须在场。
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fnotify"))
        .arg("--debounce")
        .arg("50")
        .arg(&root)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("启动 fnotify 二进制");
    let pid = child.id();
    let (tx, rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take().expect("取 stdout");
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(l) = line else { break };
            if tx.send(l).is_err() {
                break;
            }
        }
    });

    sleep(START_SETTLE);

    // 冻结分发 → 灌爆内核队列 → 解冻。事件由内核在写侧同步入队，进程
    // 停不停不影响入队，只影响读取——SIGCONT 后一次读尽即见溢出标记。
    signal_pid(pid, "STOP");
    for i in 0..flood_files {
        fs::write(root.join(format!("f{i:05}.log")), b"x").unwrap();
    }
    sleep(Duration::from_millis(300)); // 等队列状态稳定（保险垫）
    signal_pid(pid, "CONT");

    // 等首条溢出记录。排空约 1.6 万积压事件需要时间；装有实时扫描类安全
    // 软件（EDR/杀软）的主机会逐文件二次扫描，机器负载显著抬高排空耗时，
    // 预算放宽到 60s（正常主机实测数秒内即见）。
    // 行计数随收随累：通道单消费者，等循环里消费过的行结尾 try_recv 拿不到。
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut overflow_count = 0usize;
    let mut total = 0usize;
    let mut saw_overflow = false;
    while Instant::now() < deadline {
        let remain = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remain.min(Duration::from_millis(500))) {
            Ok(l) => {
                total += 1;
                if l.starts_with("{\"type\":\"overflow\"") {
                    assert!(
                        !l.contains("\"path\""),
                        "溢出记录不得携带 path 字段（type 判别，不进事件名字空间）: {l}"
                    );
                    overflow_count += 1;
                    saw_overflow = true;
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("stdout reader 提前退出"),
        }
    }
    assert!(
        saw_overflow,
        "60s 内未等到 type:\"overflow\" 降级记录（溢出仍在静默丢事件？）"
    );

    // 溢出后恢复有效：rescan 重建了根 watch，新建文件必须照常上报。
    fs::write(root.join("after-overflow.log"), b"x").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut saw_recovery = false;
    while Instant::now() < deadline {
        let remain = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remain.min(Duration::from_millis(500))) {
            Ok(l) => {
                total += 1;
                if l.contains("\"event\":\"create\"") && l.contains("after-overflow.log") {
                    saw_recovery = true;
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("stdout reader 提前退出"),
        }
    }
    assert!(
        saw_recovery,
        "溢出后新建文件的事件仍应正常上报（rescan 恢复无效？）"
    );

    let mut stderr_handle = child.stderr.take();
    let status = term_and_wait_bounded(&mut child, "溢出降级 fnotify");
    let mut err_bytes = Vec::new();
    if let Some(mut h) = stderr_handle.take() {
        use std::io::Read;
        let _ = h.read_to_end(&mut err_bytes);
    }
    let stderr = String::from_utf8_lossy(&err_bytes).into_owned();
    reader.join().expect("stdout reader");
    while let Ok(l) = rx.try_recv() {
        total += 1;
        if l.starts_with("{\"type\":\"overflow\"") {
            overflow_count += 1;
        }
    }
    assert!(
        status.success(),
        "SIGTERM 应优雅退出 exit 0，实际 {status}，stderr: {stderr:?}"
    );
    assert!(
        overflow_count >= 1,
        "stdout 应有 ≥1 条 type:\"overflow\" 行，实际 {overflow_count}，\
         stdout 共 {total} 行，stderr: {stderr:?}"
    );
    // stderr 告警行保留（恢复动作的 human 信号，M2 不动它）。
    assert!(
        stderr.contains("event queue overflow"),
        "stderr 应保留溢出告警行: {stderr:?}"
    );
}

/// s35：去抖合并的事件时间戳取末次触发（trailing-edge）。
///
/// 同一文件"写 open"（T1）与"读 open"（T2）落在同一条去抖窗口内合并为
/// 一条：修复前交付 ts=T1，后触发被吞，按 marker 时间过滤的消费端会把
/// post-marker 的读取错判为 pre-marker；修复后 ts=T2（末次），过滤即正确。
/// 去重承诺不变：同窗多次触发仍恰交付一条。
#[test]
fn s35_debounce_merge_keeps_trailing_timestamp() {
    let dir = scratch("s35");
    let o = Options {
        paths: vec![dir.0.clone()],
        recursive: false,
        globs: vec![],
        excludes: vec![],
        event_mask: engine::BIT_OPEN,
        debounce_ms: 600,
        flush_immediate: false,
        oneshot: false,
        until_idle: None,
        ready_file: None,
    };
    let e = start(o);
    sleep(START_SETTLE);

    // T1：写 open（fs::write 打开文件写入）。
    fs::write(dir.0.join("f.sh"), b"code").unwrap();
    sleep(Duration::from_millis(250));
    // marker：读动作之前的时刻（模拟消费端的 %prep 末标记时间戳）。
    let marker = engine::now_ms();
    // T2：读 open（同键第二次触发，仍在 600ms 窗口内 → 合并）。
    fs::read(dir.0.join("f.sh")).unwrap();
    // 排空：静默窗口（600ms）到期后批次交付。
    let got = collect_events(&e, "f.sh", Duration::from_millis(2500));

    assert_eq!(got.len(), 1, "同键窗口内两次触发应合并为一条: {got:?}");
    assert!(
        got[0].ts >= marker,
        "合并事件 ts 应取末次触发（≥ marker {marker}），实际 {}（首次触发时刻）",
        got[0].ts
    );
}
