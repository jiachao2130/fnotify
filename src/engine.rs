// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! 引擎：inotify 监听 → 去抖聚合 → glob/事件过滤 → 动作 worker（串行有序）。
//!
//! 线程拓扑（全同步，零 async）：
//!
//! ```text
//! 事件循环线程：mio Poll 轮询 inotify fd（read_events 非阻塞，WouldBlock 即读尽）
//!                    │ 去抖批次
//!                    ▼
//!               动作 worker 线程（串行执行 sink）
//! 主线程：轮询 Signals(SIGINT/SIGTERM)，收到信号后置 shutdown 标志，
//!         事件循环清空剩余事件、join worker，退出码 0；
//!         第二个信号 → 立即退出（128+SIGINT），避免只能 SIGKILL。
//! ```
//!
//! 退出触发：信号；任一监听目录被删除（stderr 告警后优雅退出）；
//! `--oneshot` 交付首个批次；`--until-idle` 静默期满；或动作 worker 已退出
//! （flush 失败上抛为 Err，main 转 exit 1）。

use crate::locale::pick;
use crate::pickf;
use crate::watch;
use crate::wildmatch::wildmatch;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// 事件掩码位。`-e rename` 匹配 rename_from / rename_to 全家族。
pub const BIT_CREATE: u32 = 1 << 0;
pub const BIT_MODIFY: u32 = 1 << 1;
pub const BIT_REMOVE: u32 = 1 << 2;
pub const BIT_ACCESS: u32 = 1 << 3;
pub const BIT_RENAME: u32 = 1 << 4;
pub const BIT_OTHER: u32 = 1 << 5;
/// `metadata`：元数据变更（chmod、chown、touch -m；内核 IN_ATTRIB）。
pub const BIT_METADATA: u32 = 1 << 6;
/// `close_write`：写句柄关闭（内核 IN_CLOSE_WRITE）。语义是"写事务结束"，
/// 不是"文件不会再变"——常驻 daemon 持有不关闭的写句柄时不触发。
pub const BIT_CLOSE_WRITE: u32 = 1 << 7;
/// 默认事件集：create,modify,rename,remove（rename 含 rename_from/rename_to 全家族）。
pub const DEFAULT_EVENTS: u32 = BIT_CREATE | BIT_MODIFY | BIT_RENAME | BIT_REMOVE;
const ALL_EVENTS: u32 = BIT_CREATE
    | BIT_MODIFY
    | BIT_REMOVE
    | BIT_ACCESS
    | BIT_RENAME
    | BIT_OTHER
    | BIT_METADATA
    | BIT_CLOSE_WRITE;

/// 去抖后的一批事件中的一条：一个（路径, 事件名）对。
#[derive(Clone, Debug)]
pub struct BatchItem {
    /// 本条事件首次被看到的时间（epoch 毫秒）。
    pub ts: u128,
    /// 事件名：create / modify / remove / access / metadata / close_write / rename_from / rename_to。
    pub event: &'static str,
    /// 绝对路径。
    pub path: PathBuf,
    /// 是否目录（来自内核 IN_ISDIR 标志）。
    pub dir: bool,
}

/// 引擎选项（路径需事先 canonicalize，缺失时由调用方报错退出）。
pub struct Options {
    pub paths: Vec<PathBuf>,
    pub recursive: bool,
    pub globs: Vec<String>,
    /// `--exclude` 反向过滤：命中文件名即不上报（优先于 globs）。空 = 不排除。
    pub excludes: Vec<String>,
    pub event_mask: u32,
    pub debounce_ms: u64,
    /// `--flush immediate`：每个（路径,事件名）首事件立即交付，窗口内抑制重复
    /// （leading-edge）；false 为默认的 trailing-edge 聚合。
    pub flush_immediate: bool,
    /// `--oneshot`：首个通过过滤的批次交付后走优雅停机（exit 0）。
    /// 语义级实现——内核 ONESHOT 位绕过过滤与去抖，不采用（design D13）。
    pub oneshot: bool,
    /// `--until-idle SEC`：以最近一次非空批交付为起点，静默满该时长即优雅退出；
    /// None = 不启用。持续有事件则一直运行。
    pub until_idle: Option<Duration>,
}

/// 动作 sink：CLI 接 JSONL+exec，测试接 mpsc。由 worker 线程串行调用。
pub type Sink = Box<dyn FnMut(&[BatchItem]) + Send>;

/// 解析 CLI `-e` 的单个 token；未知名返回 `None`。
pub fn event_bit_for_token(token: &str) -> Option<u32> {
    match token {
        "create" => Some(BIT_CREATE),
        "modify" => Some(BIT_MODIFY),
        "remove" => Some(BIT_REMOVE),
        "access" => Some(BIT_ACCESS),
        "rename" => Some(BIT_RENAME),
        "other" => Some(BIT_OTHER),
        "metadata" => Some(BIT_METADATA),
        "close_write" => Some(BIT_CLOSE_WRITE),
        "all" => Some(ALL_EVENTS),
        _ => None,
    }
}

/// 规范化监听路径：canonicalize（不存在报错）；去重（同一目录给两次会让摘除
/// 计数失衡）；递归模式下拒绝根之间的嵌套包含（外层递归会把内层根注册第二遍，
/// 双 wd 双报且摘除计数失衡；非递归外层不接管子目录，嵌套根合法）。
/// 返回去重后的绝对路径（保持输入顺序）。CLI 与库 API 共用同一校验。
pub fn check_roots(paths: &[PathBuf], recursive: bool) -> Result<Vec<PathBuf>, String> {
    let mut roots: Vec<PathBuf> = Vec::with_capacity(paths.len());
    for p in paths {
        match std::fs::canonicalize(p) {
            Ok(abs) => roots.push(abs),
            Err(e) => {
                return Err(pickf!(
                    "watch path does not exist: {path}: {e}",
                    "监听路径不存在: {path}: {e}",
                    path = p.display(),
                    e = e
                ));
            }
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    roots.retain(|r| seen.insert(r.clone()));
    if recursive {
        for i in 0..roots.len() {
            for j in 0..roots.len() {
                if i != j && roots[j].starts_with(&roots[i]) {
                    return Err(pickf!(
                        "watch paths must not nest: {inner} is inside {outer}",
                        "监听路径不得嵌套: {inner} 位于 {outer} 内",
                        inner = roots[j].display(),
                        outer = roots[i].display()
                    ));
                }
            }
        }
    }
    Ok(roots)
}

/// 生产入口：阻塞直到收到 SIGINT/SIGTERM，然后优雅停机。
///
/// 主线程不直接 `forever()` 阻塞，而是 200ms 轮询 `pending()`：
/// 这样引擎启动失败（如 watch 报错）能让进程立刻带错误退出，而不是干等 ctrl-c。
/// 第二个信号到达时放弃优雅停机，立即退出（128+SIGINT 惯例码），避免只能 SIGKILL；
/// 引擎汇报结果后、join 前还会最后再检查一次信号计数——清空期间到达的第二个信号同样生效。
pub fn run(opts: Options, sink: Sink) -> Result<(), String> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
    ])
    .map_err(|e| {
        pickf!(
            "failed to register signal handlers: {e}",
            "注册信号处理器失败: {e}",
            e = e
        )
    })?;

    let (done_tx, done_rx) = mpsc::channel::<Result<(), String>>();
    let engine = thread::Builder::new()
        .name("fnotify-engine".into())
        .spawn(move || {
            let _ = done_tx.send(run_with_shutdown(opts, sink, flag));
        })
        .map_err(|e| {
            pickf!(
                "failed to spawn engine thread: {e}",
                "启动引擎线程失败: {e}",
                e = e
            )
        })?;

    let mut signal_count = 0usize;
    let result = loop {
        for _ in signals.pending() {
            signal_count += 1;
        }
        exit_if_second_signal(signal_count);
        if signal_count == 1 {
            shutdown.store(true, Ordering::Release);
        }
        match done_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(res) => break res,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            // 引擎线程在汇报前崩溃：
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break Err(pick("engine thread died unexpectedly", "引擎线程异常退出").into());
            }
        }
    };
    // 引擎汇报之后再查一次信号：上一轮轮询到汇报之间到达的第二个信号不遗漏。
    for _ in signals.pending() {
        signal_count += 1;
    }
    exit_if_second_signal(signal_count);
    let _ = engine.join();
    result
}

/// 第二个信号：放弃优雅停机，立即退出（128+SIGINT 惯例码），避免只能 SIGKILL。
fn exit_if_second_signal(signal_count: usize) {
    if signal_count >= 2 {
        eprintln!(
            "fnotify: {}",
            pick(
                "second signal received, exiting immediately",
                "再次收到退出信号，立即退出"
            )
        );
        // signal-hook 提供的 _exit：不跑 atexit、不等引擎收尾。
        signal_hook::low_level::exit(130);
    }
}

/// 引擎主体（在调用线程上跑事件循环）。
///
/// 停机路径有以下几条，最后都走同一段收尾（清空 + flush + join worker）：
/// - `shutdown` 置位：事件不断时在事件分支即时做有界清空；静默时由超时分支触发；
///   持续报错时由 Err 分支触发——这三种情形下 poll_event 都可能等满整个窗口，
///   所以每个分支都要检查停机标志；
/// - 监听目录被删除：内核补发 IN_IGNORED，watcher 从 roots 摘除并记入待取队列，
///   事件循环逐目录告警、继续监听其余目录；全部摘光才优雅退出；
/// - `--oneshot`：首个通过过滤的批次交付后优雅退出；
/// - `--until-idle`：以最近一次非空批交付为起点，静默期满优雅退出；
/// - 动作 worker 已退出（sink panic 等）：任一 flush 发送失败即停止监听，返回 Err（exit 1）。
///
/// watcher 报错不算停机：仅在 stderr 告警，继续运行。
pub fn run_with_shutdown(
    opts: Options,
    sink: Sink,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    if opts.paths.is_empty() {
        return Err(pick(
            "at least one watch root is required",
            "至少需要一个监听目录",
        )
        .into());
    }
    if opts.debounce_ms == 0 {
        return Err(pick(
            "debounce window must be greater than 0",
            "去抖窗口必须大于 0",
        )
        .into());
    }

    let mut watcher = watch::Watcher::new(opts.recursive, opts.event_mask).map_err(|e| {
        pickf!(
            "failed to create watcher: {e}",
            "创建 watcher 失败: {e}",
            e = e
        )
    })?;
    for root in &opts.paths {
        watcher.add_root(root).map_err(|e| {
            pickf!(
                "failed to watch {path}: {e}",
                "监听 {path} 失败: {e}",
                path = root.display(),
                e = e
            )
        })?;
    }

    // 事件循环 → 动作 worker。有界通道（积压上限 64 批）：慢 sink（如 webhook
    // 单批最长 10s）+ 持续事件流时 send 阻塞形成背压，inotify 队列随之积压。
    // 背压本身只造成投递延迟；积压到内核 inotify 队列溢出时事件在内核侧
    // 丢弃，走 Q_OVERFLOW 重建路径（watch 全量重建、丢失期间的变化不补偿，
    // 见 watch.rs）——比无限积压耗尽内存可取。
    let (btx, brx) = mpsc::sync_channel::<Vec<BatchItem>>(64);
    let worker = thread::Builder::new()
        .name("fnotify-worker".into())
        .spawn(move || {
            let mut sink = sink;
            while let Ok(batch) = brx.recv() {
                sink(&batch);
            }
        })
        .map_err(|e| {
            pickf!(
                "failed to spawn action thread: {e}",
                "启动动作线程失败: {e}",
                e = e
            )
        })?;

    let window = Duration::from_millis(opts.debounce_ms);
    let immediate = opts.flush_immediate;
    let oneshot = opts.oneshot;
    let until_idle = opts.until_idle;
    let mut agg = Aggregator::new(opts.globs, opts.excludes, opts.event_mask, immediate);
    // 动作 worker 是否已退出（flush 发送失败）：置位后停止监听、以 Err 收尾。
    let mut worker_gone = false;
    // watcher 连续报错计数：持续报错（如 fd 损坏）时退避，防热旋与 stderr 洪泛。
    let mut err_streak = 0u32;
    loop {
        // IN_IGNORED 在上一次 poll_event 内部同步处理，已删除的监听目录在这里取走。
        // 逐目录摘除：每个被删目录告警一行，其余目录继续监听；全部摘光才退出。
        let mut all_roots_gone = false;
        while let Some(root) = watcher.next_removed_root() {
            eprintln!(
                "fnotify: {}",
                pickf!(
                    "watch root removed: {path}, no longer watching",
                    "监听目录已被删除: {path}，不再监听",
                    path = root.display()
                )
            );
            if watcher.no_roots() {
                all_roots_gone = true;
            }
        }
        if all_roots_gone {
            eprintln!(
                "fnotify: {}",
                pick(
                    "all watch roots removed, exiting",
                    "全部监听目录已被删除，退出"
                )
            );
            break;
        }
        match watcher.poll_event(window) {
            Ok(Some(ev)) => {
                err_streak = 0;
                agg.push(ev.name, &ev.path, ev.is_dir, window);
                if immediate {
                    // leading-edge：每个（路径,事件名）首事件直接交付。
                    if !agg.flush(&btx) {
                        worker_gone = true;
                        break;
                    }
                    // 无批可清（事件已随达随交），置位即退；剩余事件由收尾读尽处理。
                    if shutdown.load(Ordering::Acquire) {
                        break;
                    }
                } else if shutdown.load(Ordering::Acquire) {
                    // 事件不断时 poll_event 不会超时返回，停机要在这里处理。
                    worker_gone = !drain_bounded(&mut watcher, &mut agg, window, &btx);
                    break;
                }
                // 批次时长封顶：事件持续到达（间隔 < 窗口）时，批次延迟以窗口为界。
                if !agg.flush_if_aged(window, &btx) {
                    worker_gone = true;
                    break;
                }
                if oneshot && agg.delivered {
                    eprintln!(
                        "fnotify: {}",
                        pick(
                            "oneshot: first batch delivered, exiting",
                            "oneshot：首个批次已交付，退出"
                        )
                    );
                    break;
                }
            }
            Ok(None) => {
                err_streak = 0;
                if !agg.flush(&btx) {
                    worker_gone = true;
                    break;
                }
                if shutdown.load(Ordering::Acquire) {
                    break;
                }
                if oneshot && agg.delivered {
                    eprintln!(
                        "fnotify: {}",
                        pick(
                            "oneshot: first batch delivered, exiting",
                            "oneshot：首个批次已交付，退出"
                        )
                    );
                    break;
                }
                // until-idle：完整静默窗口已过才到这里——以最近交付为起点计时。
                if let Some(idle) = until_idle {
                    if agg.last_delivery.elapsed() >= idle {
                        eprintln!(
                            "fnotify: {}",
                            pickf!(
                                "idle for {sec}s, exiting",
                                "事件静默满 {sec}s，退出",
                                sec = idle.as_secs()
                            )
                        );
                        break;
                    }
                }
            }
            Err(e) => {
                err_streak += 1;
                eprintln!(
                    "fnotify: {}",
                    pickf!(
                        "watcher error (continuing): {e}",
                        "watcher 报错（继续运行）: {e}",
                        e = e
                    )
                );
                // 持续报错时退避（10ms 起翻倍，封顶 640ms），不热旋。
                thread::sleep(Duration::from_millis(10 << err_streak.min(6)));
                // 持续报错同样可能永远等不到超时，停机也要在这里检查。
                if shutdown.load(Ordering::Acquire) {
                    worker_gone = !drain_bounded(&mut watcher, &mut agg, window, &btx);
                    break;
                }
            }
        }
    }
    // 优雅收尾：剩余事件经 drain_bounded 有界清空（总时长封顶 2×window，
    // 出现一个空窗口提前结束）。持续写入流（如 tail -f > watched/x）下
    // poll_event(ZERO) 永远读得到新事件，无界 drain 会让 oneshot/until-idle
    // 挂在收尾不退出（信号路径有二次 Ctrl-C 兜底，oneshot 没有）。
    worker_gone |= !drain_bounded(&mut watcher, &mut agg, window, &btx);
    if worker_gone {
        // flush 发送失败即 worker 已退出：不再监听，join 只是回收线程，
        // 错误信息按 worker 退出来报。
        drop(btx);
        drop(watcher);
        let _ = worker.join();
        return Err(pick(
            "action worker is gone, stopping watch",
            "动作 worker 已退出，停止监听",
        )
        .into());
    }
    drop(btx); // 关闭 worker 通道，worker 清空后退出
    drop(watcher); // 显式先于函数返回销毁，关闭事件源

    match worker.join() {
        Ok(()) => Ok(()),
        Err(_) => Err(pick("action thread died unexpectedly", "动作线程异常退出").into()),
    }
}

/// 停机清空：总时长封顶 2×window，保证繁忙流下停机延迟有界。
/// 期间到达的事件继续按批次时长封顶交付；出现一个空窗口则提前结束。
/// 返回 false 表示动作 worker 已退出（发送失败），调用方应立即停止监听。
fn drain_bounded(
    watcher: &mut watch::Watcher,
    agg: &mut Aggregator,
    window: Duration,
    btx: &mpsc::SyncSender<Vec<BatchItem>>,
) -> bool {
    let deadline = Instant::now() + window.saturating_mul(2);
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            break;
        }
        match watcher.poll_event(remain) {
            Ok(Some(ev)) => {
                agg.push(ev.name, &ev.path, ev.is_dir, window);
                if agg.immediate {
                    if !agg.flush(btx) {
                        return false;
                    }
                } else if !agg.flush_if_aged(window, btx) {
                    return false;
                }
            }
            Ok(None) => break, // 空窗口：清空完成
            Err(e) => eprintln!(
                "fnotify: {}",
                pickf!(
                    "watcher error (continuing): {e}",
                    "watcher 报错（继续运行）: {e}",
                    e = e
                )
            ),
        }
    }
    agg.flush(btx)
}

/// 去抖聚合器：把原始事件按 (path × 事件名) 去重累积进当前批次，
/// 并记录批次开启时刻（用于繁忙流下的批次时长封顶）。
/// `immediate` 为 true 时不做批内累积：每个（路径,事件名）首事件直接入批，
/// 抑制表保证一个窗口内同键只交付一次。
struct Aggregator {
    batch: Vec<BatchItem>,
    seen: HashSet<(PathBuf, &'static str)>,
    since: Option<Instant>,
    globs: Vec<String>,
    excludes: Vec<String>,
    mask: u32,
    immediate: bool,
    /// 抑制表：已交付的（路径,事件名）键 → 抑制期满时刻（immediate 模式专用）。
    suppressed: HashMap<(PathBuf, &'static str), Instant>,
    /// 是否已交付过非空批（oneshot 的判定依据）。
    delivered: bool,
    /// 最近一次非空批交付时刻（until-idle 的计时起点；构造时刻 = 引擎启动）。
    last_delivery: Instant,
}

impl Aggregator {
    fn new(globs: Vec<String>, excludes: Vec<String>, mask: u32, immediate: bool) -> Self {
        Aggregator {
            batch: Vec::new(),
            seen: HashSet::new(),
            since: None,
            globs,
            excludes,
            mask,
            immediate,
            suppressed: HashMap::new(),
            delivered: false,
            last_delivery: Instant::now(),
        }
    }

    fn push(&mut self, name: &'static str, path: &Path, is_dir: bool, window: Duration) {
        if event_bit(name) & self.mask == 0 {
            return;
        }
        if !passes(&self.globs, &self.excludes, path) {
            return;
        }
        if self.immediate {
            // leading-edge：键在抑制期内则丢弃，否则立即入批并记抑制期满时刻。
            let key = (path.to_path_buf(), name);
            let now = Instant::now();
            if let Some(exp) = self.suppressed.get(&key) {
                if *exp > now {
                    return;
                }
            }
            if self.suppressed.len() > 4096 {
                // 抑制表只做去抖用途，条目过期即无价值，顺手清理。
                self.suppressed.retain(|_, exp| *exp > now);
            }
            self.suppressed.insert(key, now + window);
            self.batch.push(BatchItem {
                ts: now_ms(),
                event: name,
                path: path.to_path_buf(),
                dir: is_dir,
            });
            return;
        }
        if self.seen.insert((path.to_path_buf(), name)) {
            if self.batch.is_empty() {
                self.since = Some(Instant::now());
            }
            self.batch.push(BatchItem {
                ts: now_ms(),
                event: name,
                path: path.to_path_buf(),
                dir: is_dir,
            });
        }
    }

    /// 发出当前批次并清空去重表。空批不发送。
    /// 发送成功且批次非空时记交付（oneshot / until-idle 的判定依据）。
    /// 返回 false 表示动作 worker 已退出（发送失败），调用方应停止监听。
    fn flush(&mut self, btx: &mpsc::SyncSender<Vec<BatchItem>>) -> bool {
        if self.batch.is_empty() {
            return true;
        }
        let sent = btx.send(std::mem::take(&mut self.batch)).is_ok();
        if sent {
            self.delivered = true;
            self.last_delivery = Instant::now();
        }
        self.seen.clear();
        self.since = None;
        sent
    }

    /// 批次时长达到窗口即交付：事件持续到达（间隔小于窗口）时，批次延迟以窗口为界。
    /// 返回值即 [`Self::flush`] 的结果（未触发交付时恒为 true）。
    fn flush_if_aged(&mut self, window: Duration, btx: &mpsc::SyncSender<Vec<BatchItem>>) -> bool {
        if self.since.map(|t| t.elapsed() >= window).unwrap_or(false) {
            self.flush(btx)
        } else {
            true
        }
    }
}

/// 上报放行判定：（无 globs 或任一命中文件名）且（无 excludes 或无一命中）。
/// exclude 优先于 include 由此而来；只影响上报，不影响 watch 注册。
fn passes(globs: &[String], excludes: &[String], path: &Path) -> bool {
    (globs.is_empty() || glob_hit(globs, path))
        && (excludes.is_empty() || !glob_hit(excludes, path))
}

/// 任一 glob 命中文件名（路径最后一段）即通过。
fn glob_hit(globs: &[String], path: &Path) -> bool {
    let Some(name) = path.file_name() else {
        return false;
    };
    let name = name.to_string_lossy();
    globs.iter().any(|g| wildmatch(g, &name))
}

/// 事件名对应的掩码位（rename_from/rename_to 归入 rename）。
fn event_bit(name: &str) -> u32 {
    match name {
        "create" => BIT_CREATE,
        "modify" => BIT_MODIFY,
        "remove" => BIT_REMOVE,
        "access" => BIT_ACCESS,
        "rename" | "rename_from" | "rename_to" => BIT_RENAME,
        "metadata" => BIT_METADATA,
        "close_write" => BIT_CLOSE_WRITE,
        _ => BIT_OTHER,
    }
}

/// 当前 epoch 毫秒（时间戳取源：事件聚合与 exec 生命周期共用）。
pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// epoch 毫秒 → RFC3339 UTC（`2026-10-16T09:05:00.123Z`，毫秒恒为三位）。
/// 手写 civil 换算（Hinnant 算法），不引 chrono：形状固定，零新依赖。
pub fn format_ts(ms: u128) -> String {
    let secs = (ms / 1000) as u64;
    let millis = (ms % 1000) as u32;
    let days = (secs / 86400) as i64;
    let rem = (secs % 86400) as u32;
    let (hour, min, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

/// days from epoch → (year, month, day)，month/day 均从 1 起。
/// Hinnant 的 civil_from_days 换算，常数 719468/146097 等出自其论文。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// 单条事件 JSONL：`type` 判别字段先行，ts 为 RFC3339 UTC。
pub fn jsonl_line(item: &BatchItem) -> String {
    format!(
        "{{\"type\":\"event\",\"ts\":\"{}\",\"event\":\"{}\",\"path\":\"{}\",\"dir\":{}}}",
        format_ts(item.ts),
        json_escape(item.event),
        json_escape(&item.path.to_string_lossy()),
        item.dir
    )
}

/// exec 生命周期记录（与事件同流，判别字段同为 `type`）。
/// 纯函数构造，便于单测；发出方负责 writeln 到 stdout。
pub fn exec_start_line(ts: u128, pid: u32, cmd: &str) -> String {
    format!(
        "{{\"type\":\"exec_start\",\"ts\":\"{}\",\"pid\":{pid},\"cmd\":\"{}\"}}",
        format_ts(ts),
        json_escape(cmd)
    )
}

/// exec_end：`exit`/`signal` 依退出形态取值（正常退出给 exit，被信号杀死给
/// signal），状态不可得时均为 null；`timed_out` 标记是否被超时组杀。
pub fn exec_end_line(
    ts: u128,
    pid: u32,
    cmd: &str,
    exit: Option<i32>,
    signal: Option<i32>,
    timed_out: bool,
    duration_ms: u128,
) -> String {
    let exit = exit.map_or("null".to_string(), |c| c.to_string());
    let signal = signal.map_or("null".to_string(), |s| s.to_string());
    format!(
        "{{\"type\":\"exec_end\",\"ts\":\"{}\",\"pid\":{pid},\"cmd\":\"{}\",\"exit\":{exit},\"signal\":{signal},\"timed_out\":{timed_out},\"duration_ms\":{duration_ms}}}",
        format_ts(ts),
        json_escape(cmd)
    )
}

/// 手写 JSON 字符串转义：`"`、`\`、常见控制字符、`\u00XX`。
pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        Aggregator, BIT_CREATE, BIT_RENAME, BatchItem, DEFAULT_EVENTS, exec_end_line,
        exec_start_line, format_ts, json_escape, jsonl_line, passes,
    };
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn json_escape_quotes_and_backslashes() {
        assert_eq!(json_escape("plain"), "plain");
        assert_eq!(json_escape("a\"b"), "a\\\"b");
        assert_eq!(json_escape("a\\b"), "a\\\\b");
    }

    #[test]
    fn check_roots_dedupes_and_rejects_recursive_nesting() {
        use super::check_roots;
        let base = std::env::temp_dir().join(format!("fnotify-roots-{}", std::process::id()));
        let outer = base.join("outer");
        let inner = outer.join("inner");
        let other = base.join("other");
        std::fs::create_dir_all(&inner).unwrap();
        std::fs::create_dir_all(&other).unwrap();

        // 去重：同一目录两次 → 一份
        let r = check_roots(&[outer.clone(), outer.clone(), other.clone()], false).unwrap();
        assert_eq!(r.len(), 2);

        // 递归模式下嵌套拒绝（CLI exit 2 / 库 Error::Config 共用的 Err 路径）
        assert!(check_roots(&[outer.clone(), inner.clone()], true).is_err());
        // 非递归外层不接管子目录 → 嵌套根合法
        assert!(check_roots(&[outer.clone(), inner.clone()], false).is_ok());
        // 前缀相近但非包含（组件边界）不误判
        let sibling = base.join("outer2");
        std::fs::create_dir_all(&sibling).unwrap();
        assert!(check_roots(&[outer.clone(), sibling.clone()], true).is_ok());
        // 不存在的路径报错
        assert!(check_roots(&[base.join("nope")], false).is_err());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn json_escape_named_control_chars() {
        assert_eq!(json_escape("\n\r\t"), "\\n\\r\\t");
        assert_eq!(json_escape("\u{08}\u{0c}"), "\\b\\f");
    }

    #[test]
    fn json_escape_other_c0_as_unicode_escape() {
        assert_eq!(json_escape("\u{00}\u{01}\u{1f}"), "\\u0000\\u0001\\u001f");
    }

    #[test]
    fn json_escape_passes_through_lossy_replacement_char() {
        // 非 UTF-8 路径经 to_string_lossy 变成 U+FFFD；它不是控制字符，原样输出。
        assert_eq!(json_escape("a\u{FFFD}b"), "a\u{FFFD}b");
    }

    #[test]
    fn jsonl_line_shape() {
        let item = BatchItem {
            ts: 123,
            event: "create",
            path: PathBuf::from("/tmp/x\"y\\z"),
            dir: true,
        };
        assert_eq!(
            jsonl_line(&item),
            "{\"type\":\"event\",\"ts\":\"1970-01-01T00:00:00.123Z\",\"event\":\"create\",\"path\":\"/tmp/x\\\"y\\\\z\",\"dir\":true}"
        );
    }

    #[test]
    fn format_ts_known_anchors() {
        // 已知的 epoch → RFC3339 锚点，覆盖跨年、闰日、毫秒进位。
        assert_eq!(format_ts(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_ts(86_400_000), "1970-01-02T00:00:00.000Z");
        assert_eq!(format_ts(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
        assert_eq!(format_ts(951_782_400_000), "2000-02-29T00:00:00.000Z");
        // 毫秒不进位到秒。
        assert_eq!(format_ts(999), "1970-01-01T00:00:00.999Z");
    }

    #[test]
    fn exec_lifecycle_lines_shape() {
        let start = exec_start_line(0, 42, "logger 'a\"b'");
        assert_eq!(
            start,
            "{\"type\":\"exec_start\",\"ts\":\"1970-01-01T00:00:00.000Z\",\"pid\":42,\"cmd\":\"logger 'a\\\"b'\"}"
        );
        // 正常退出：exit 有值、signal 为 null、timed_out 为 false。
        let end = exec_end_line(1_000, 42, "true", Some(0), None, false, 12);
        assert_eq!(
            end,
            "{\"type\":\"exec_end\",\"ts\":\"1970-01-01T00:00:01.000Z\",\"pid\":42,\"cmd\":\"true\",\"exit\":0,\"signal\":null,\"timed_out\":false,\"duration_ms\":12}"
        );
        // 超时组杀：exit 为 null、signal 为 9、timed_out 为 true。
        let killed = exec_end_line(2_000, 43, "sleep 9", None, Some(9), true, 1_000);
        assert_eq!(
            killed,
            "{\"type\":\"exec_end\",\"ts\":\"1970-01-01T00:00:02.000Z\",\"pid\":43,\"cmd\":\"sleep 9\",\"exit\":null,\"signal\":9,\"timed_out\":true,\"duration_ms\":1000}"
        );
        // 状态不可得：exit 与 signal 均为 null。
        let unknown = exec_end_line(3_000, 44, "x", None, None, false, 5);
        assert!(
            unknown.contains("\"exit\":null,\"signal\":null"),
            "{unknown}"
        );
    }

    #[test]
    fn filter_passes_three_combinations() {
        // 仅 -g：命中通过、未命中丢弃。
        assert!(passes(&["*.log".into()], &[], Path::new("/tmp/a.log")));
        assert!(!passes(&["*.log".into()], &[], Path::new("/tmp/a.txt")));
        // 仅 --exclude：未命中通过、命中丢弃（"除了这些全都要"）。
        assert!(passes(&[], &["*.tmp".into()], Path::new("/tmp/a.log")));
        assert!(!passes(&[], &["*.tmp".into()], Path::new("/tmp/a.tmp")));
        // -g 与 --exclude 同给：exclude 优先于 include。
        assert!(!passes(
            &["*.log".into()],
            &["a.*".into()],
            Path::new("/tmp/a.log")
        ));
        assert!(passes(
            &["*.log".into()],
            &["a.*".into()],
            Path::new("/tmp/b.log")
        ));
        // 两者都空：全通过。
        assert!(passes(&[], &[], Path::new("/tmp/anything")));
    }

    #[test]
    fn aggregator_push_respects_excludes() {
        // exclude 命中即不上报；globs 与 excludes 都不命中才累积。
        let (tx, rx) = mpsc::sync_channel(64);
        let mut agg = Aggregator::new(
            vec!["*.log".into()],
            vec!["skip-*".into()],
            DEFAULT_EVENTS,
            false,
        );
        let window = Duration::from_millis(50);
        agg.push("modify", &PathBuf::from("/tmp/a.log"), false, window);
        agg.push("modify", &PathBuf::from("/tmp/skip-a.log"), false, window);
        agg.push("modify", &PathBuf::from("/tmp/b.txt"), false, window);
        assert!(agg.flush(&tx));
        let batch = rx.recv().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].path, PathBuf::from("/tmp/a.log"));
    }

    #[test]
    fn aggregator_push_respects_event_mask() {
        // 掩码外的事件（access）被丢弃，掩码内（create、rename 家族）累积。
        let (tx, rx) = mpsc::sync_channel(64);
        let mut agg = Aggregator::new(Vec::new(), Vec::new(), BIT_CREATE | BIT_RENAME, false);
        let window = Duration::from_millis(50);
        agg.push("access", &PathBuf::from("/tmp/a"), false, window);
        agg.push("create", &PathBuf::from("/tmp/a"), false, window);
        agg.push("rename_from", &PathBuf::from("/tmp/a"), false, window);
        assert!(agg.flush(&tx));
        let batch = rx.recv().unwrap();
        assert_eq!(batch.len(), 2);
        assert!(batch.iter().any(|it| it.event == "create"));
        assert!(batch.iter().any(|it| it.event == "rename_from"));
    }

    #[test]
    fn aggregator_flush_if_aged_delivers_under_busy_stream() {
        // 时长封顶：持续 push（间隔 < 窗口、静默窗口永不出现）时，批次时长满一个窗口
        // 即由 flush_if_aged 交付，不等 poll_event 超时。
        let (tx, rx) = mpsc::sync_channel(64);
        let mut agg = Aggregator::new(Vec::new(), Vec::new(), DEFAULT_EVENTS, false);
        let window = Duration::from_millis(50);
        agg.push("create", &PathBuf::from("/tmp/busy/0"), false, window); // 开批，since = 现在
        let start = Instant::now();
        let mut i = 1usize;
        while start.elapsed() < window {
            // 每次换路径：同一个（路径, 事件名）在批内去重时只算一条，模拟不出繁忙流。
            agg.push(
                "create",
                &PathBuf::from(format!("/tmp/busy/{i}")),
                false,
                window,
            );
            i += 1;
            thread::sleep(Duration::from_millis(1));
        }
        assert!(agg.flush_if_aged(window, &tx), "发送成功应返回 true");
        let batch = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("批次时长 ≥ 1 窗口，应已交付（静默窗口从未出现）");
        assert!(!batch.is_empty());
        assert!(batch.iter().all(|it| it.event == "create"));
        // 交付后批次复位：紧接着再查时长不应重复交付。
        assert!(agg.flush_if_aged(window, &tx));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn aggregator_flush_if_aged_holds_young_batch() {
        // 反向断言：批次时长未满窗口时不交付——时长封顶不是立即 flush。
        let (tx, rx) = mpsc::sync_channel(64);
        let mut agg = Aggregator::new(Vec::new(), Vec::new(), DEFAULT_EVENTS, false);
        agg.push(
            "create",
            &PathBuf::from("/tmp/young"),
            false,
            Duration::from_millis(50),
        );
        assert!(agg.flush_if_aged(Duration::from_secs(10), &tx));
        assert!(rx.try_recv().is_err(), "批次时长未满窗口，不应交付");
    }

    #[test]
    fn aggregator_immediate_suppresses_repeats_within_window() {
        // leading-edge：首事件立即入批；窗口内同（路径,事件名）键被抑制；
        // 抑制期满（超出窗口）后可再次交付；不同事件名互不影响。
        let (tx, rx) = mpsc::sync_channel(64);
        let mut agg = Aggregator::new(Vec::new(), Vec::new(), DEFAULT_EVENTS, true);
        let window = Duration::from_millis(50);
        let path = PathBuf::from("/tmp/hot.log");

        agg.push("modify", &path, false, window);
        assert!(agg.flush(&tx));
        let batch = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event, "modify");

        // 窗口内同键重复：抑制，无批次。
        agg.push("modify", &path, false, window);
        assert!(agg.flush(&tx));
        assert!(rx.try_recv().is_err(), "窗口内重复事件应被抑制");

        // 同路径不同事件名：不受抑制表影响。
        agg.push("create", &path, false, window);
        assert!(agg.flush(&tx));
        let batch = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event, "create");

        // 抑制期满后同键恢复交付。
        thread::sleep(Duration::from_millis(60));
        agg.push("modify", &path, false, window);
        assert!(agg.flush(&tx));
        let batch = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event, "modify");
    }
}
