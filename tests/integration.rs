// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! 集成测试：s1–s27 共 27 个场景，直连内核 inotify。
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
    let deadline = Instant::now() + WAIT;
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
                    "删除监听目录后 {WAIT:?} 内 fnotify 未退出"
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

    // 删掉根 B（最后一个）：引擎应优雅退出。
    fs::remove_dir_all(&dir_b.0).unwrap();
    let handle = e.handle.take().expect("引擎线程句柄");
    let deadline = Instant::now() + WAIT;
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "全部监听目录删除后引擎未在 {WAIT:?} 内退出"
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
