# 设计方案：fnotify —— Linux 文件事件路由器

> 设计定稿：Linux-only、最低工具链见 Cargo.toml 的 rust-version（main 为 edition 2024；
> `msrv-1.82` 分支保留 1.82 基线）、RPM 交付、极简依赖；
> 跨平台已随 D1 反转明确放弃（requirements 非目标首行），TOML 配置 / CI /
> crates.io 评估结论见 §9。
> 原则：直接依赖白名单 5 个（inotify / mio / signal-hook / toml / libc）+ std；同步线程模型，全项目零 async；MSRV 由 rust-version 声明 + Cargo.lock 保障
> 文中所有数字来自 x86_64 Linux 实测，复现命令见 §8/§11。

---

## §0 目标与非目标

需求口径见 [requirements.md](requirements.md)（FR-01…11 / NFR-01…6 / 验收矩阵）。本文只讲**怎么实现与为什么这样实现**。

## §1 总体架构

```text
          ┌────────────────────────────── fnotify 进程（3 线程，无库内部线程）──────────────────────────────┐
          │                                                                                              │
 内核     │      引擎线程 fnotify-engine                    动作 worker（串行）                                │
 inotify ─┼─▶  watch::Watcher（mio Poll 轮询 fd） ──批次──▶  Sink: Box<FnMut>                              │
          │      非阻塞读尽 / wd 映射维护 /        Aggregator 去重/时长封顶        │   ├─ JSONL → stdout    │
          │      目录接管 / 事件分类              poll_event(window) 聚合         │   └─ exec → sh -c      │
          │                ▲                                                                        │
          │  主线程：Signals 轮询（200ms pending）──  AtomicBool shutdown                                │
          │    第 1 个 SIGINT/SIGTERM → 置位，引擎有界清空后收尾 exit 0                                   │
          │    第 2 个信号 → signal_hook::low_level::exit(130)（不跑 atexit）                          │
          │    IN_IGNORED 命中监听目录 → stderr 告警 + 优雅退出                                          │
          └──────────────────────────────────────────────────────────────────────────────────────────────┘
```

要点（全部经实测或测试固化）：

- **单写者原则**：`Watcher`（`src/watch.rs`）与 inotify fd 同属引擎线程，无跨线程共享；
  引擎线程独占 `Aggregator`；worker 独占 sink。全链路无锁（`Arc<AtomicBool>` 是唯一共享状态），无 `Mutex`。
- **拓扑来源**：事件源不用 notify 的库线程 + mpsc 通道，改为引擎线程内直接
  mio 轮询 inotify fd——`poll_event` 三态（Ok / Timeout / Err）承接通道原来的
  三态（Ok / Timeout / Disconnected）停机协议，语义一一对应（§12）。
- **库/壳分离**：`engine::run(Options, Sink)` 是纯库入口（测试注入 mpsc sink），
  `main` 只是一层薄壳（canonicalize、CLI、组装 sink，约百行）。继承自 anotify 最成功的决策。

## §2 核心决策表

| # | 决策 | 替代方案 | 依据 |
|---|---|---|---|
| D1 | 事件源直接 `inotify = 0.11.5` + `mio` 轮询，自建 watcher 层（`src/watch.rs`） | `notify = 8.2.0`（备选，已否决：重评后认为 anotify 死因是四缺陷的防护栏缺失而非自研 watcher 本身）/ 裸 inotify-sys | 否决理由：曾认为"自研递归是 anotify 死因"，重评后认定死因是四缺陷的**防护栏缺失**而非自研本身——wd 映射、递归铺开、新建子目录接管、移出清理集中在一个模块后，连同 s7/s8/s9 集成固化；收益：依赖树 14→8、去掉 watcher 线程与 mpsc 通道、notify 双报产生的泛化 rename 及其批内去重代码整体删除。inotify 0.11.5 MSRV 1.70 ✓；`=0.11.5` 精确锁，mio 锁 minor（1.71） |
| D2 | 同步线程模型（3 线程 + mpsc） | tokio（anotify 原方案） | 并发单元仅 2–3 个（1 事件源 + N 动作 + 1 信号），tokio 的收益区间在千级并发 I/O；实测 RSS 2.3MB vs anotify(tokio) 3.8MB（1.82 地板时期另有"tokio 1.54+ 超线"的锁版论据，地板抬升后决策靠架构论据独立成立） |
| D3 | `rust-version` 声明 + Cargo.lock 锁版 + 精确 pin | 追新；或仅声明 rust-version | 地板见 Cargo.toml（单一事实源）；main 用 cargo 的 MSRV 感知解析（`.cargo/config.toml` resolver fallback，cargo ≥1.84 稳定），`cargo update` 自动回避超线版本；1.82 兼容基线由 `msrv-1.82` 分支冻结承担（该分支保留原 rationale：RPM 构建链 = 系统 rustc，源码包构建不可升级，且 Cargo 1.82 无 MSRV 感知解析） |
| D4 | 手写 CLI 解析（~120 行含 help） | clap `~4.5`（锁版可用） | flag 面 ~10 个；预算"直接依赖 ≤ 3" |
| D5 | 手写 `wildmatch`（`* ? [...] [!...]`，~50 行 + 10 单测） | `glob` crate（零传递）/ `regex` 一族（5+ crate） | glob 语义覆盖文件名匹配 90% 场景；regex 树违背 NFR-02 且引入"匹配全路径还是文件名"语义坑（anotify 实测踩过） |
| D6 | 手写 JSONL 转义（~20 行 + 5 单测） | serde_json（4–5 crate，MSRV 无虞） | 输出形状固定、`type` 判别先行、零 serde；预算优先 |
| D7 | trailing-edge 去抖 + 批次时长封顶 | leading-edge（立即发）/ 无去抖（anotify） | 监控场景要的是聚合降噪（实测 S3：203 vs 1003 行）；时长封顶保证繁忙流下批次延迟以窗口为界（s5 回归固化）；`--flush immediate` 已落地（s13 固化）：每个（路径,事件名）首事件立即交付，窗口内同键抑制 |
| D8 | exec `%p` POSIX 单引号包裹（`'` → `'\''`） | 直接拼接 / shell数组 | 实测：文件名 `inj;touch-evil.log` 无法注入；`%e` 出自封闭常量集 |
| D9 | 监听目录被删 → 逐目录摘除、摘光才整体优雅退出 | 任一被删整体退出（备选）/ 僵尸空转（anotify 实测行为） | 长驻场景删一个根不应拖死其余；s6（单根退出）+ s15（摘除后继续监听、摘光退出）+ 实机冒烟 |
| D10 | Mulan PSL v2（源文件头 + Cargo/spec 声明） | MIT（脚手架自带）/ Apache-2.0 | 对齐工作交付惯例（suoji 同款），中英双语文本见 LICENSE |
| D11 | `close_write` 掩码层 opt-in：不订阅则内核不产出、引擎连读都不读 | 恒订阅常开（默认事件集加成员） | 与 `access` 同模式（watch 掩码按 `event_mask` 置位）；默认事件集与 JSONL 契约零变化是硬约束 |
| D12 | `--exclude` 在聚合器上报层过滤，不参与 watch 注册 | 注册层排除（不挂 watch） | 注册层排除会让新建子目录接管、目录删除清理的 wd 映射维护漏掉被排除路径，状态机复杂度不值；上报层过滤是同一信息的纯函数裁剪 |
| D13 | `--oneshot` 语义级实现（交付首个非空批后走既有优雅停机收尾） | 内核 `IN_ONESHOT` 位 | 内核版绕过过滤与去抖——首个内核事件即炸毁 watch，与"首个通过过滤的批次"语义冲突（anotify 同名 flag 即此语义） |
| D14 | 库 API 嵌入 = 同步回调 sink，扇出由调用方 spawn | broadcast channel 扇出（anotify 形态） | 引擎的 Sink 契约本来就是串行回调，embed 直接复用零成本；channel 扇出把并发模型强加给调用方，与 D2 同步线程模型同构 |

## §3 引擎详细设计

### 3.1 数据结构（与 `src/engine.rs` 同步维护）

```rust
pub struct Options {          // 引擎输入（main 组装，路径已 canonicalize）
    pub paths: Vec<PathBuf>,  // 监听目录（≥1）
    pub recursive: bool,      // -r → 递归注册整棵子树
    pub globs: Vec<String>,   // -g 可重复；空 = 全通过
    pub excludes: Vec<String>, // --exclude 可重复；命中即不上报（优先于 globs）
    pub event_mask: u32,      // BIT_CREATE|BIT_MODIFY|…（-e 编译产物）
    pub debounce_ms: u64,     // 静默窗口，CLI 层已保证 ≥10
    pub flush_immediate: bool, // --flush immediate → leading-edge 交付（默认 false）
    pub oneshot: bool,        // --oneshot：首个非空批次交付后走优雅停机
    pub until_idle: Option<Duration>, // --until-idle SEC：批次交付刷新计时，静默期满优雅退出
    pub ready_file: Option<PathBuf>, // --ready-file PATH：全部根挂表完成后创建的空文件（一次性就绪信号，退出不删除）
}

pub struct BatchItem {        // 去抖批次中的一条 = (路径, 事件名) 对
    pub ts: u128,   // 本条首次被看到的 epoch 毫秒（不是 flush 时刻）；序列化为 RFC3339 UTC
    pub event: &'static str,  // create/modify/remove/access/open/metadata/close_write/rename_from/rename_to
    pub path: PathBuf,        // 绝对路径（监听目录已 canonicalize，子路径由 wd+name 拼出）
    pub dir: bool,            // 内核 IN_ISDIR 标志
}

pub type Sink = Box<dyn FnMut(&[BatchItem]) + Send>;  // worker 线程串行调用

struct Aggregator {           // 引擎线程独占
    batch: Vec<BatchItem>,    // 当前批次
    seen: HashMap<(PathBuf, &'static str), usize>,  // 批内去重键 → 批内下标（合并回写末次 ts）
    since: Option<Instant>,   // 批次开启时刻（时长封顶依据）
    globs: Vec<String>, excludes: Vec<String>, mask: u32,
    delivered: bool,          // 是否已交付过非空批（oneshot / until-idle 的判定依据）
    // push / flush / flush_if_aged
}

// src/watch.rs（inotify 封装，与引擎线程同生命周期）：
pub struct Watcher {
    ino: Inotify, poll: Poll, events: Events,
    mask: WatchMask,          // CREATE|DELETE|MODIFY|ATTRIB|MOVED_*|ONLYDIR（+ACCESS/OPEN/CLOSE_WRITE 视 -e）
    wd_map: HashMap<WatchDescriptor, PathBuf>,   // wd → 目录路径
    by_path: HashMap<PathBuf, WatchDescriptor>,  // 反向索引：移出/删除时按路径撤 watch
    roots: Vec<PathBuf>,      // 队列溢出后按此重建
    removed_roots: VecDeque<PathBuf>,  // IN_IGNORED 命中监听目录，引擎取走后退出
    pending: VecDeque<RawEvent>,
    pending_moves: HashMap<u32, PathBuf>,  // MOVED_FROM|ISDIR 暂存（cookie→旧路径）：
                                           // 配对 MOVED_TO = 树内 rename（改写前缀映射）；
                                           // 批末读尽无配对 = 移出监听树（整棵摘除）
}
```

事件掩码用 `u32` 位集（`BIT_CREATE..BIT_OPEN`），`-e all` = 全置位；token→bit 的映射是
封闭函数 `event_bit_for_token`，非法 token 在 CLI 层拒绝（exit 2）。

过滤判定的完整规则（`Aggregator::push`）：事件掩码位未订阅 → 丢弃；`globs`
非空且无一命中 → 丢弃；`excludes` 任一命中 → 丢弃。三道判定语义为
（掩码内）且（无 globs 或 globs 命中）且（无 excludes 或 excludes 未命中），
exclude 优先于 include 即由此而来。过滤只作用于上报，不影响 watch 注册。

### 3.2 事件循环与停机协议

主线程 `run()`：200ms 轮询 `Signals::pending()`（不 `forever()` 阻塞——引擎启动失败要能立刻
带错误退出）；`done_rx.recv_timeout(200ms)` 等引擎汇报。信号计数 ≥2 → `low_level::exit(130)`；
引擎汇报结果后、join 前还会最后再检查一次信号计数——清空期间到达的第二个信号同样生效。

引擎线程 `run_with_shutdown` 主循环 `watcher.poll_event(window)` 三态：

启动序列（进入主循环前）：逐根 `add_root`（递归模式铺完整棵子树）→ 全部成功后
按 `ready_file` 创建就绪空文件（一次性行为，此后不再写；创建失败仅 stderr 告警
不中断，退出时也不删除——生命周期归消费端）→ 起 worker 线程 → 进入事件循环。
任一根挂表失败即返回 Err（exit 1），此时就绪文件不会创建；因此"文件出现"即
"全部 watch 已生效"，此后的文件事件不会因挂表未完成而丢失——消费端以轮询该
文件替代 `sleep 1` 赌挂表；选文件而非 stdout 记录是为了不向
事件流混入异构行。

- `Ok(Some(ev))`：`shutdown` 已置位 → `drain_bounded`（总时长硬封顶 2×window，期间按时长
  继续交付）后收尾；否则 `aggregator.push(ev)` + `flush_if_aged()`
  （批次时长 ≥ 1 window 即交付——**这是繁忙流延迟有界的关键**，s5 回归固化）。
- `Ok(None)`（窗口超时）：`flush()` 批次交 worker；若 `shutdown` 置位 → 收尾。
  未配置 oneshot 且 `until_idle` 超时（以最近一次非空批交付为起点的静默期满）→
  同样走收尾，exit 0。oneshot 在任何分支交付非空批后置位即收尾。
  循环下一轮开头先消费 `watcher.next_removed_root()`：IN_IGNORED 在上一次 poll 内部
  同步处理，已删除的监听目录统一在循环顶取走——超时分支同样能触发目录删除退出。
- `Err(e)`：watcher 报错仅 stderr 告警继续运行；`shutdown` 已置位时同样走 `drain_bounded`
  收尾——持续报错流永远等不到超时返回，不停机检查的话停机永远不会被处理（与 §12
  的繁忙流去抖同理）。

任一 `flush` / `flush_if_aged` / `drain_bounded` 发现发送失败（动作 worker 已退出，如 sink
panic）→ 主循环立即 break，引擎返回 Err（exit 1），不再空转丢批。

收尾固定顺序：`poll_event(ZERO)` 读尽 fd 剩余事件 → flush 末批 → drop worker 发送端 →
join worker（不丢末批、无死锁）。

### 3.3 去抖语义（trailing-edge + 时长封顶 + rename 批内去重）

| 机制 | 行为 | 动机 |
|---|---|---|
| 静默窗口 | 窗口内无新事件才交付批次 | 聚合降噪（S3 实测 203 vs 1003 行） |
| 批内去重 | `(路径,事件名)` 键，每批每键一次；合并事件 ts 取窗口内末次触发（trailing-edge，s35） | `touch`+写+关 = 一批一行 create + 一行 modify；首 ts 会把同窗 post-marker 读取错判为 pre-marker，且"代表最近一次活动"与静默窗口语义自洽 |
| 时长封顶 | 事件持续到达时，最老条目满 1 window 强制交付 | 繁忙日志目录批次延迟不以流量为函数（s5） |
| 停机清空 | `drain_bounded` ≤ 2×window 硬上限 | 优雅退出不会被繁忙流拖住（s5），双信号兜底 130 |
| rename 语义 | 裸 `IN_MOVED_FROM`/`IN_MOVED_TO` 直接映射 `rename_from`/`rename_to`，不做 cookie 配对 | 内核一次 mv 天然两条（notify 时代双报 4 条、需批内去重的历史已随 D1 反转消失）；树内 mv / mv 出 / mv 入路径行为由 s8 固化 |

### 3.4 事件分类映射（inotify 0.11.5，mask → 事件名）

| 内核 mask | 事件名 | 说明 |
|---|---|---|
| `IN_CREATE` | `create` | 目录创建且递归时同批接管：先挂 watch 再扫描子树（add-before-scan 不变式，扫描期间新建的子目录由父目录 watch 的后续 CREATE 兜底，不丢目录；s28） |
| `IN_MODIFY` | `modify` | 内容写入 |
| `IN_ATTRIB` | `metadata` | 元数据变更（chmod/chown/touch -m）；自 modify 细分独立，默认事件集不含（s16 固化） |
| `IN_MOVED_FROM` | `rename_from` | mv 旧路径；若是已接管的目录则同批撤掉旧 watch（防过期路径与双报） |
| `IN_MOVED_TO` | `rename_to` | mv 新路径；若是移入的目录且递归则同批接管（同 add-before-scan 不变式，s28） |
| `IN_DELETE` | `remove` | 删除；被删目录的 watch 由内核补发 `IN_IGNORED` 统一清理 |
| `IN_ACCESS` | `access` | 仅 `-e access` 时订阅（默认掩码不含，读操作不产生事件） |
| `IN_OPEN` | `open` | 仅 `-e open` 时订阅（默认掩码不含）。成功 open(2) 即入队，天然覆盖 mmap 读（`IN_ACCESS` 只覆盖 read(2) 类访问）；过计数与 open+access 双报语义见下文准入判据段（s29） |
| `IN_CLOSE_WRITE` | `close_write` | 仅 `-e close_write` 时订阅。语义是"写事务结束"（写句柄关闭），不是"文件不会再变"：上传类工具（cp/scp/rsync）写完即关，可视为本轮写入完成；daemon 常驻、句柄从不关闭的进程写的文件永不触发 |
| `IN_IGNORED` | （内部） | watch 摘除：清 wd 映射（同路径已重建并指向新 wd 的迟到 IGNORED 不盲删新表项）；命中监听目录则从 roots 摘除并记入待取队列，引擎逐目录告警 |
| `IN_MOVE_SELF` | （内部） | 监听目录自身被移动：暂存，批末按映射是否被配对改写裁决——移出监听树按删除同语义摘除（s17）；树内 rename 不动 |
| `IN_Q_OVERFLOW` | （内部，输出 `type:"overflow"` 降级记录） | 队列溢出：stderr 告警 + 按记录的全部监听目录重建 watch + stdout 一条仅含 `ts` 的降级行（绕过订阅掩码/glob/去抖三道闸、不进事件名字空间，每次溢出各一条；s34） |

未订阅即不产生：`CLOSE` / `CLOSE_NOWRITE` / `MOVE_SELF` / `DELETE_SELF` / `UNMOUNT`。
`MOVE_SELF` 空缺即"监听目录被 mv 后按过期路径继续上报"这一已知限制（§5）。

事件分类学的准入判据：新事件类必须携带**文件生命周期层面**、且现有事件类
**无法近似**的信息。`close_write` 据此准入——`IN_MODIFY` 只说明"写过"，
"写句柄已关闭"是上传收尾判定的唯一可靠信号，`modify` 无法近似。`open`
准入为 opt-in 事件（默认集不含，需 `-e open` 订阅）：`IN_ACCESS` 只覆盖
read(2) 类访问，clang 等编译器经 mmap 读源码/头文件整类漏计，`IN_OPEN` 在
成功 open 时入队、天然覆盖 mmap；过计数
评估为可接受——open+close 存在性探测、execve（exec 视为参与构建语义正确）
都计入，open 失败不产生 `IN_OPEN` 无误报；read() 类文件 open+access 双报
属预期（不做事件合并，消费端按 path 去重）。`close_nowrite` 仍不订阅：
纯关闭不携带可用信息。

实测：一次 `echo x > f` = create + modify 两类；一次 `mv` = `rename_from` +
`rename_to` 两条，无泛化冗余——notify 时代的双报与批内去重随 D1 反转整体移除。

**扫描期掩码取舍（add-before-scan 的代价面）**：递归注册/接管的 walker
先挂 watch 再 readdir，扫描自己的 opendir/getdents 会以子目录名在**父目录**
的 watch 上自触发 `IN_ACCESS`/`IN_OPEN`（部分挂载下读目录更新 atime 还自
触发 `IN_ATTRIB`）。对策：扫描期以摘除 ACCESS/OPEN/ATTRIB 的掩码挂表
（CREATE/MOVED_* 全保留，不变式只依赖目录类事件），子树 DFS **后序**逐
目录升级回完整掩码（同 wd 原地改掩码，不产生事件；前序升级会让万级树
一次下钻的 2×N 自触发事件灌爆 16384 内核队列，overflow→rescan→再灌爆
自激并楔死单线程 dispatch，s32 实证；OPEN 不摘则启动扫描自带幻影 open，
s33）。readdir 失败分支同样先升级再返回（瞬态失败下目录仍可 watch，纵深
防御；机制钉子为单测 `scan_mask_watch_upgrades_to_full_mask`）。代价：
目录自身的 ACCESS/OPEN/ATTRIB 盲窗 = 其**整个子树的扫描期**（文件事件
不受影响——文件从不参与扫描）。

## §4 CLI 文法与语义

```ebnf
cli      := [flag]* path+ EOF                    # path 默认 "."
flag     := "-r" | "-q" | "-h" | "-V" | ("-g" | "--glob" | "--exclude" | "-x"
          | "--exec" | "-e" | "--events" | "--debounce" | "--flush" | "--jobs"
          | "--timeout" | "--webhook" | "--config" | "--until-idle" | "--ready-file") value
          | "--oneshot" | "-rq" 等布尔短 flag 粘连（仅限 r/q/h/V 组合）
value    := 内联("--flag="v) 或下一个 argv
```

| flag | 语义 | 违规处理 |
|---|---|---|
| `-r/--recursive` | 递归整棵子树 | |
| `-g/--glob` | 文件名通配，可重复，任一命中通过 | 空值 exit 2 |
| `--exclude` | 反向文件名通配，可重复，命中即不上报；优先于 `-g` | 空值 exit 2 |
| `-e/--events` | 逗号列表，值域见 3.4；默认 `create,modify,rename,remove` | 未知值 exit 2 |
| `--debounce` | 静默窗口 ms，默认 50 | <10 按 10 处理；非数字 exit 2 |
| `--flush` | `debounce`（默认）或 `immediate`（首事件立即交付，窗口内同键抑制） | 非法值 exit 2 |
| `-x/--exec` | 命令模板，`%p`/`%e` 占位 | 空值 exit 2 |
| `--jobs` | exec 并发上限，默认 1 串行 | 非数字/超 u32 exit 2；0 按 1 |
| `--timeout` | 单 exec 超时秒数，默认 0 不限；超时按进程组杀 | 非数字 exit 2 |
| `--webhook` | 批次以 JSON 数组 POST 到 http:// 端点 | 非 http/主机非法 exit 2（启动时校验） |
| `--oneshot` | 首个通过过滤的批次交付后优雅停机（exit 0） | |
| `--until-idle <SEC>` | 每交付一批重置计时，静默满 SEC 秒优雅退出；持续事件则存活 | <1 / 非数字 exit 2 |
| `--ready-file` | 全部根挂表完成后、进入事件循环前创建空文件（一次性就绪信号）；退出不删除（生命周期归消费端）；启动失败不创建 | 空值 exit 2；创建失败仅告警 |
| `--config` | TOML 配置文件作解析基线，命令行优先 | 读不到/语法错/未知键 exit 2 |
| `-q/--quiet` | 关 JSONL（常与 -x 连用） | |
| `-` | 字面量路径（`dash_is_positional` 固定） | |

`--oneshot` 是语义级实现（引擎在交付首个非空批后走既有优雅停机收尾），
不用内核 `IN_ONESHOT` 位——内核版绕过过滤与去抖，语义与 anotify 的同名 flag
不同。`--until-idle` 计时以非空批交付为刷新点（引擎启动时刻为初始值），
与内核事件到达时刻无关。两者同给时先满足者生效。

布尔短 flag 可粘连：`-rq` 等价 `-r -q`（含取值型短 flag 或未知字母仍按未知选项 exit 2）。
监听路径规范化后去重；递归模式下根之间不得嵌套包含（子目录会被注册两遍），违反 exit 2。
`--` 终结符对 `--config` 预扫描同样生效（`--` 后的 `--config=...` 按路径处理）；已知边角：
`--config --` 的取值以 `-` 开头会被拒绝（GNU getopt 会收下 `--` 作 optarg）——
预存在差异，无实际场景，记录不修。

退出码：0 正常（含信号优雅停机、根删除/移出）；1 运行错误（watch 注册失败、worker 崩溃）；2 用法错误。

## §5 稳定性边界清单

| 场景 | 行为 | 依据 |
|---|---|---|
| 非 UTF-8 文件名 | `to_string_lossy`，不 panic；JSONL 仍合法（U+FFFD 原样输出）；glob 侧：含字面字符的模式永不命中（U+FFFD 替换了原字节），`*`/`?` 仍可通过 | 单测 + 代码评审通过 |
| 未闭合 `[` / `[!...]` | 按字面量匹配 | `unclosed_bracket_is_literal` |
| 事件风暴 | `IN_Q_OVERFLOW` 告警后按磁盘现状重建 watch（重建亦走 add-before-scan，重建期间新建的目录不丢；溢出期被删的目录走删除退出语义，不留零 watch 僵尸）；批次有界（时长封顶）；溢出同时向消费端发 `type:"overflow"` 降级记录（绕过三道闸、每次溢出各一条，静默丢事件变为可显式感知的降级） | 20000 事件 anotify 对照实验 + s5 + s34 |
| 空 glob/exec 值 | exit 2（不再静默过滤一切） | `errors_exit_path` |
| 引擎线程崩溃 | 主线程 `Disconnected` 检出，exit 1 | run() 协议 |
| worker sink panic | `join()` 转错误，exit 1；flush 发送失败即停止监听返回 Err，不空转 | 集成 s11（panic sink 端到端）+ §3.2 |
| 目录被 rename/移动 | 树内 rename：cookie 配对后 wd 映射按前缀改写，事件落新路径（s10）；子树移出监听树：批末读尽裁决，整棵摘除后代 watch，不以旧路径误报（s9 含后代层）；监听目录**自身**被移走：`IN_MOVE_SELF` 批末裁决——映射未被配对改写 = 移出监听树，按删除同语义摘除（roots 除名 + 告警，s17）；已改写 = 嵌套根的树内 rename，不动 | watch.rs `pending_moves`/`moved_self`/`remove_subtree`；D9 范围内 |
| 相对路径根 | 启动即 canonicalize 为绝对（anotify 相对路径递归崩溃的结构性免疫） | FR-01 |

## §6 依赖策略

```toml
[dependencies]
inotify = "=0.11.5"    # 事件源：内核 inotify 封装（fd 生命周期 + 事件解析）；递归注册与 wd 映射自研（见 D1）
mio = { version = "1.2", default-features = false, features = ["os-poll", "os-ext"] }  # epoll 轮询 inotify fd，替代 watcher 线程
signal-hook = "0.4"    # 信号安全处理；0.4.5 MSRV 1.66
toml = { version = "~1.0", default-features = false, features = ["std", "parse", "serde"] }
# TOML 配置文件（--config）；~1.0 锁 minor（1.76）。DOM API（Table/Value）在 serde
# feature 下；只开 std/parse/serde，display/indexmap 等可选依赖不进构建。
libc = "0.2"           # kill(2) 进程组击杀：本就在传递树中（inotify 依赖），提升为
# 直接依赖替代外部 kill(1) 调用（不经 PATH 查找、无 shell-out 开销）；MSRV 1.63
# 传递链 toml → serde_core / serde_spanned / toml_datetime / toml_parser / winnow，
# 全链 MSRV ≤ 地板（见 Cargo.toml rust-version；锁文件固化）；仓库根 .cargo/config.toml 启用
# resolver.incompatible-rust-versions = "fallback"，cargo update 自动回避超线版本。
```

**不引入清单**（每项附理由）：`tokio`（D2：规模错位）、
`clap`（D4）、`regex`（D5）、`glob`（D5：能用但手写 50 行换零依赖更合算）、
`ureq`/`rustls`（webhook 已用手写最小 HTTP 客户端落地 http://；https 需要 TLS 栈时再
评审，ureq 3.3 MSRV 1.85 在 main 地板以内，引入时再评估）、
`path-absolutize`（std canonicalize 入口一次足矣——anotify 的它恰是相对路径
崩溃链一环）。Linux 全树 14 crate（`cargo tree --target x86_64-unknown-linux-gnu` 实测，
口径为依赖 crate 不含 fnotify 自身：notify 时代 14 → 反转直连 inotify 后 8（去掉
notify/notify-types/log/walkdir/same-file）→ 引入 TOML 配置后 14（净增 toml 门面与
serde_core/serde_spanned/toml_datetime/toml_parser/winnow，均 MSRV ≤ 地板）。

## §7 测试计划

| 层 | 数量 | 内容 |
|---|---|---|
| 单测 | 87 | wildmatch 11（含边界固定测试）、cli 26（解析/合并语义 20 + 中英一致性 3 + 事件名合法值表护栏 1 + `-` 字面路径 1 + ready-file 解析 1）、config 9（加载、未知键/类型错、复用 CLI 取值规则、空串拒绝、新键映射）、engine 16（json_escape 4 + jsonl_line 形状 + overflow 降级行形状 + format_ts 锚点 + exec 生命周期记录形状 + Aggregator 5（含合并取末次 ts） + 过滤三组合/排除 2 + check_roots 1）、hook 3（端点解析含控制字符回显转义断言、JSON 数组正文体、5xx 判败）、locale 4、watch 9（映射/cookie 配对 4 + ENOSPC 提示 2 + 迟到 IGNORED 防盲删 1 + 非递归 rescan 只挂根 1 + 扫描掩码升级机制 1）、main 9（shell_quote 1 + 模板渲染 2 + exec 池 6）；含 main 侧单测 |
| 集成 | 35 | s1 非递归生命周期（含 IN_ISDIR dir 断言）/ s2 glob 过滤（屏障法负断言）/ s3 去抖聚合 / s4 递归 / s5 繁忙流停机有界（回归）/ s6 exec 端到端（恶意文件名逐字节 + `-q` stdout 为空负断言）+ 目录删除退出 / s7 多目录 + 运行中新建子目录接管 / s8 rename_from/rename_to 语义 / s9 子树移出后引擎存活且后代无误报（回归）/ s10 树内 rename 前缀改写 + 旧路径重建接管（锐利形态回归）/ s11 worker panic 引擎以 Err 退出（exit 1 锚定）/ s12 路径不存在 exit 2（FR-01 端到端）/ s13 immediate 抑制 / s14 immediate 繁忙流停机有界 / s15 逐目录摘除 / s16 metadata 细分 / s17 监听目录移出摘除 / s18 exec 并发与超时 / s19 webhook 端到端 / s20 --config 与命令行覆盖 / s21 exec 生命周期记录上屏（type 判别 + pid 关联）/ s22 close_write 慢写者论证（≥2 modify 批 + 恰 1 条，对照运行 0 条）/ s23 exclude 优先于 include（屏障法负断言）/ s24 oneshot 无信号有界退出 + 全过滤负断言 / s25 until-idle 时序（持续事件不退 + 静默期满退）/ s26 oneshot 持续写入流有界退出（无间隔紧密 writer 贯穿断言窗口，红绿实测坐实防线）/ s27 递归 EACCES 子树告警一行且其余子树照常 / s28 创建风暴接管完整性（万级目录 mv 入监听树 + 接管窗口内持续新建 late 目录：零缺失断言 + 大树抽样对照，红绿实测坐实）/ s29 open 事件 opt-in（只开不读恰 1 条 open / read() 双报 open+access / python3 mmap 读有 open 无 access / 默认集对照 0 条）/ s30 ready-file 正常路径（就绪文件出现后事件零丢失、SIGTERM 排空后文件仍在）/ s31 ready-file 反例（根不存在 exit 2 → 就绪文件永不创建）/ s32 创建风暴跨内核队列上限（20000 目录 mv 入根：零缺失 + stderr 零 overflow + SIGTERM 有界退出，红态为前序升级自激楔死）/ s33 open 启动扫描零幻影（-e open -r 排空零事件，开后恰 1 条 file open）/ s34 内核队列溢出降级记录（SIGSTOP + 2 万文件强制真实 IN_Q_OVERFLOW：type:"overflow" 行 ≥1 且无 path + 默认订阅掩码旁路实证 + 溢出后新建文件照常上报 + SIGTERM 优雅退出）/ s35 去抖合并取末次时间戳（写 open 与读 open 同窗合并为一条、ts ≥ 读前 marker，红绿实测坐实） |
| lib API | 6 | `tests/lib_api.rs`：事件值域复用、路径缺失错误分类、until_idle(0) 拒绝、递归嵌套根拒绝（与 CLI 同表）、公开面端到端交付（只用 `fnotify::` 前缀，轮询等就绪）、ready_file 经 Config builder 生效 |
| 实机 | 每发布 | Linux 实机：离线构建 + inotify 冒烟（全生命周期 + 双信号 + 目录删除）+ rpmbuild（§11） |

稳定性纪律：修改任何引擎行为前先加回归测试（s5 即一次缺陷复现的固化）；三遍全绿才算过；
全部测试在 Linux 跑同一套。

## §8 基准计划与实测基线

口径：同机同负载对照 fnotify 0.1.0 vs anotify（last build, 3.9MB）；场景 S1 突发（500 建 + 500 改 + 250 删）、
S2 递归（2 子目录 200 文件）、S3 持续（10s 内 1000 次追加）；CPU 为 10ms tick 粗测，drain 为 300ms 轮询粗测。

| 指标 | fnotify | anotify | 备注 |
|---|---|---|---|
| 二进制 | **0.1.0 实测 859,928 B（840 KB；edition 2024 工具链）** | 3.9 MB | lto+strip；≤1MB 门槛达标 |
| 首事件延迟（默认） | 51 ms | 3 ms | fnotify = 50ms 窗口 + ~1ms 管道；`--debounce 10` 实测 **11–14 ms**（三连测），纯管道开销 2–4ms |
| 首事件延迟（`--flush immediate`） | **2–3 ms**（三连测，含外部轮询开销） | — | 引擎看到事件本身 +0~1ms（JSONL ts 与写入前打点同帧）；immediate 交付 ≈ 纯管道开销，达成 ≤ 5ms 目标（s13 固化语义） |
| S1 捕获率 | **100%**（501/501/250） | 100%（501/1001/250） | anotify 1001 行 MODIFY 为内核原始流（建文件写入也计一次） |
| S1 CPU | 20 ms | 10 ms | ~11µs vs ~6µs 每事件，绝对值可忽略 |
| S3 输出行数 | **203** | 1003 | 聚合 5× 缩减 |
| 峰值 RSS | **2.1–2.3 MB** | 3.7–3.8 MB | 无 tokio 运行时 |
| 递归捕获 | 100% | 100%（仅绝对路径 + 静态树前提） | anotify 四缺陷见 §12 |

**验收门槛（NFR-03）**：RSS ≤ 3MB ✓（0.1.0 实测 2.2MB）；二进制 ≤ 1MB ✓（0.1.0 实测 840 KB）；持续写输出 ≤ 原始 1/4 ✓（0.20）。
延迟不设门槛（设计参数，非开销）；`--flush immediate` 交付延迟 2–3ms，达成 ≤ 5ms 目标。

> 实测附记（同口径多轮复测，结果稳定）：延迟（51/11ms）、RSS、S1/S2 捕获率与上表
> 持平，S3 输出 167–203 vs 1000 行（同口径波动）；受控场景 RSS 2064–2248 KB，
> S1 突发与 S3 持续写零增长；108 例测试 dist 包内三遍全绿；RPM 构建并冒烟通过。

## §9 库 API 承诺与 crates.io 评估

**公开面（0.x 实验级）**：库 API 只经 `fnotify::` 前缀暴露——`Config`（构造器风格，
覆盖 CLI 全集）、`run(config, sink)` / `run_with_shutdown(config, sink, flag)`、
`Event`（即引擎批次条目）、`Error`（手写枚举）。嵌入即同步回调 sink，扇出由
调用方自行 spawn；不暴露 Watcher / inotify 内部；不做 async。
内部模块（cli/config/engine/hook/locale/watch/wildmatch）经 `#[doc(hidden)]`
公开——技术上可达（二进制 crate 复用同一实现），但属约定边界而非承诺 API，
升 1.0 时可自由收紧为私有。
0.x 期间语义可随版本演进、不做兼容承诺（破坏性变更在 CHANGELOG 明示）；
升 1.0 前冻结当前形态并补文档与元数据。

| crates.io 维度 | 结论 |
|---|---|
| 名称 | `fnotify` 实测可用（crates.io API 404，2026-10-16 查证）；近名 fanotify（内核 API）有混淆风险，GitHub 有 Feuerlabs/fnotify（Erlang inotify 绑定，不占 crates.io 命名空间）——已评估可接受 |
| 价值 | 分发已有 RPM + 源码包双通道；`cargo install` 对内部工具的边际价值有限 |
| 成本 | 发布即把实验级 API 钉成事实公共接口，维护兼容是持续负担 |
| 决定 | 维持 `Cargo.toml` 的 `publish = false`，暂不发布；若将来重启，前置条件 = API 1.0 冻结 + 元数据补齐（repository/homepage/documentation） |

## §10 风险与缓解

| 风险 | 概率 | 缓解 |
|---|---|---|
| 生态 MSRV 漂移（clap 4.6 / toml 1.1 / ureq 3.3 / tokio 1.54 已集体 1.85） | 确定（进行中） | 锁版策略：不追新 major；依赖升级必伴随地板工具链 `cargo test --locked` 全绿；仓库根 `.cargo/config.toml` 已启用 `resolver.incompatible-rust-versions = "fallback"`，cargo（≥1.84）update 时自动回避超线版本——toml 传递链即靠此锁在地板以下的一系列；引入新依赖前仍先查 MSRV |
| `fs.inotify.max_user_watches` 耗尽（默认 8192/65536） | 中（大树） | 直连 inotify 无 PollWatcher 式回退（notify 已移除）：注册失败按根报错（exit 1）而非静默降级；已落人话提示——报错带出当前配额值与 sysctl 修法（enospc_hint）；分批注册未做，大树仍需调大 sysctl |
| inotify/mio 小版本事件语义漂移 | 低（inotify `=0.11.5`、mio 锁 minor） | 锁版；升级视为破坏性变更走全套测试 |

## §11 构建与交付（实测链路）

```sh
make check   # cargo fmt --check && cargo clippy --all-targets -D warnings && cargo test（Linux；macOS 编不了，D1 后仅可编辑）
make msrv    # cargo +<地板> check --locked（Linux 机器；地板见 Cargo.toml rust-version）
make vendor  # cargo vendor --locked + build/.cargo offline 配置
make dist    # dist/fnotify-0.1.0.tar.gz（src+tests+lock+vendor+.cargo+rpm；打包前 xattr -cr 清 mac 元数据）
```

一键出 SRPM：`bash scripts/make-srpm.sh`（staging 组装离线 vendor 后
`rpmbuild -bs`，全程无网）。手动流程：`%_topdir` 实测为 rpm 内置默认
`%{getenv:PWD}`——随执行时 cwd 变化（在 /tmp 下执行
则落到 `/tmp`；无 `~/.rpmmacros` 覆盖时）。
**构建前 `rpm --eval '%_topdir'` 确认落点，固定 cwd 后执行 rpmbuild**。
tarball → `~/SOURCES/`，仓库根的 `fnotify.spec` → `~/SPECS/`，`rpmbuild -bb`；`%build` =
`cargo build --release`（vendored 依赖随源码包，全程无网；spec 声明
`BuildRequires: rust`）。
产物：`~/RPMS/x86_64/fnotify-0.1.0-1.<dist>.x86_64.rpm`（dist 标签随构建系统；
`rpm -qip` 可见 License: MulanPSL-2.0）。spec 无 `rust-toolset`/
`%{rust_arches}`（构建环境无此宏/包，系统 rustc 满足地板即可，见 Cargo.toml）。

## §12 关键设计取舍的实证依据

> anotify 前身四缺陷（实测）：① 相对路径递归路径双拼（`anotify -R -c ./a-bug`
> 立即 exit 1）；② 子目录移出后 `MOVED_FROM` 误判加 `?` 传播，mv 即 exit 1；
> ③ 监听目录删除 3 秒后进程仍存活（僵尸空转）；④ 错误写 stdout、stderr 恒为空。
> 事件风暴 20000 例侥幸存活（无 panic、计数精确）——直通管道每事件开销确实略优
> （§8），但无防护栏。fnotify 的免疫分别来自：启动时 canonicalize 加全树注册
> （watch.rs）、`pending_moves` cookie 配对与整棵摘除、IN_IGNORED 根摘除、
> NFR-06 的 stderr 纪律。

> notify 8.2 泛化 rename 双报（实测一次 mv 同投"具体 + 泛化"两路共 4 行）——
> 跨平台抽象层在本项目只剩 Linux 路径在用，却带来双报、通道断开语义、线程边界
> 三笔额外复杂度。这是 D1 反转（直连内核 inotify，`inotify 0.11.5` 管 fd 与解析、
> `mio 1.2` epoll 轮询，依赖树 14 → 8 crate）的实证动机之一。直连后 rename 不再有
> 泛化事件，`dir` 取 IN_ISDIR，根删除检测走 IN_IGNORED 路径匹配；事件状态维护
> （wd 映射、目录接管、IGNORED/Q_OVERFLOW）收敛进 `src/watch.rs`，与引擎同线程、
> 按批同步处理——anotify 的死因重判为"防护栏缺失"而非"自研 watcher"。

> 子树移出的 wd 生命周期（s9/s10 固化）：`MOVED_FROM|ISDIR` 只撤顶层 wd 会让
> 后代内核 watch 残留在 inode 上，树外活动以旧路径继续误报。修法是
> `pending_moves`（cookie→旧路径）：配对 MOVED_TO = 树内 rename，走
> `rewrite_prefix` 前缀改写（wd 随 inode 存活，仅换映射路径）；批末读尽无配对 =
> 移出监听树，走 `remove_subtree` 整棵摘除。树内 rename 后 `by_path` 旧键残留
> 会在原地重建同名子树时误判已接管，随 `rewrite_prefix` 一并消除。

> 繁忙流去抖的健壮性要求：flush 与 shutdown 检查不能只在 Timeout 分支——否则
> 持续事件流下批次交付与停机都被无限期拖延（实机复现：持续写入加预置 shutdown，
> 5 秒零交付）。引擎 Ok 分支同样查 shutdown、`flush_if_aged` 时长封顶、
> `drain_bounded` 有界清空（s5/s14 回归固化）。

> 测量纪律：任何"等首个事件"的测量必须带超时上限，且写入前先等 watcher 注册
> ——注册窗口前的事件内核不补发（`tests/lib_api.rs` 的轮询等就绪即此教训的固化）。

> 双语消息的 locale 约定（src/locale.rs）：仅 fnotify 自有消息（help/usage 横幅
> 加错误消息模板）跟随 locale，strerror 等 OS 动态文本恒英文；检测链
> `FNOTIFY_LANG`（仅认精确 zh/en）> LC_ALL > LC_MESSAGES > LANG，首个非空值
> `zh*` 前缀 → 中文、其余英文兜底，OnceLock 进程内缓存一次；横幅经 `pick` 二选一、
> 带占位符的消息经 `pickf!` 宏。
