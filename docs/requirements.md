# 需求文档：fnotify —— Linux 文件事件路由器

> 原则：每条需求都附带验收方式与实测出处；验收以 [design.md](design.md) §8 的实测基线为准。
> 背景：fnotify 是 anotify（2023，已归档于 `../anotify`）的重写。anotify 的四个必现缺陷见 design.md §12。

## §0 定位与背景

**一句话定位**：把"文件发生了什么"变成"该发生的事"——监听目录 → 去抖聚合 → glob 过滤 →
输出动作（JSONL / exec），零依赖安装的单二进制。

| 项 | 值 |
|---|---|
| 产品形态 | 单二进制 CLI（无 daemon；网络能力仅限可选的 http webhook） |
| 目标平台 | Linux x86_64（rustc 1.82，RPM 交付）；不支持其他平台 |
| 前身 | anotify（async-inotify），架构思路继承，代码零复用 |
| 交付物 | fnotify 0.1.0 二进制 + `fnotify-0.1.0-1.<dist>.x86_64.rpm`（实测构建） |

范围：Linux-only + RPM 交付 + MSRV 1.82 + 极简依赖，一次性交付全部功能
（TOML 配置、CI、webhook、库 API 同版落地）；跨平台明确放弃（design D1 后定位 Linux-only）。

## §1 目标与非目标

**目标（全部已实现并实测）：**

1. 在 Linux 上以 rustc 1.82 离线构建出 RPM，全程不访问网络（vendor + `--offline`）。
2. 正确捕获监听目录内的文件生命周期事件（create / modify / remove / rename 家族），
   递归与非递归两种模式，**事件零丢失**（实测 S1/S2 场景 100% 捕获）。
3. 结构化输出（JSONL）与命令触发（exec）两种动作，输出量经去抖聚合显著低于原始事件流
   （实测持续写入场景 203 行 vs 原始 1003 行）。
4. 极简依赖：直接依赖 5 个 crate（inotify / mio / signal-hook / toml / libc）、Linux 全依赖树
   14 个 crate（实测 `cargo tree` 计数，全部 MSRV ≤ 1.82；libc 本就在传递树中，
   提升为直接依赖仅为 kill(2) 进程组击杀）。

**非目标（明确不做）：**

| 不做 | 理由 |
|---|---|
| 跨平台支持（macOS/Windows/BSD） | 不做；事件源直接依赖 inotify（Linux 专有系统调用），仓库不保证其他平台编译 |
| daemon / Web | 不做，fnotify 保持"单命令即用"；配置文件已落地（`--config FILE`，TOML，命令行优先，s20 固化） |
| webhook | 已落地：`--webhook URL` 按批次 POST JSON 数组（仅 http://，手写最小 HTTP 客户端、无 TLS 依赖；https 需引入 TLS 栈再评审，s19 固化） |
| 库 API（crates.io 发布、公共接口稳定性） | 引擎经 `lib.rs` 可复用，但 API 为 0.x 实验级、未承诺稳定；评估结论：暂不发布 crates.io，维持 `publish = false`，详见 design §9 |
| inotify 原始掩码粒度（open 等细分） | 8 类事件（create / modify / remove / access / metadata / close_write / rename_from / rename_to）；`close_write` 掩码层 opt-in（s22 固化）；open / close_nowrite 不携带生命周期信息，不订阅（准入判据见 design §3.4） |
| exec 无界并发 | 不做：并发池有界（`--jobs` 默认 1 串行、池满背压，可预测、防雪崩，s18 固化） |
| CI 之外的托管依赖 | CI 已落地：GitHub Actions 双工具链（stable + 1.82）fmt + 全量测试；本地 `make ci` 门禁合集 |

## §2 功能需求（FR）

每条附验收方式：`[测试]` = 仓库内测试（名称），`[实测]` = Linux 实机操作记录（design.md §8/§12）。

| # | 需求 | 验收方式 |
|---|---|---|
| FR-01 | 监听一个或多个目录；监听路径启动时 `canonicalize` 一次，不存在/不可访问则报错退出（码 2） | [测试] `errors_exit_path`、集成 s1、s12（不存在路径 exit 2 端到端）；[实测] 冒烟 |
| FR-02 | `-r` 递归监听（含启动前已存在的整棵子树） | [测试] 集成 s4；[实测] S2 场景 200 文件 100% 捕获 |
| FR-03 | 事件分类与订阅：`-e` 逗号列表，合法值 `create,modify,remove,access,rename,metadata,close_write,other,all`（权威清单见 src/cli.rs `EVENT_CHOICES`，本表以它为准）；默认 `create,modify,rename,remove`；非法值 exit 2。`close_write` 掩码层 opt-in：不含于 `-e` 则不订阅、事件不产出；语义为"写事务结束"非"不会再变" | [测试] `events_all`、`events_metadata_is_distinct`、`defaults`、`errors_exit_path`、集成 s16、s22（慢写者论证：≥2 个 modify 批 + 恰好 1 条 close_write；对照运行 0 条） |
| FR-04 | 去抖聚合：静默窗口（`--debounce`，默认 50ms，最小 10ms）内事件聚合为一批；批内按 `(路径,事件名)` 去重；事件持续到达时批次时长以一个窗口为封顶，**繁忙流下批次延迟有界** | [测试] `aggregator_flush_if_aged_*` 2 个单测（deterministic）、集成 s3（聚合）、s5（繁忙流停机有界，回归测试） |
| FR-05 | rename 语义：输出 `rename_from`（旧路径）/ `rename_to`（新路径），一次 mv 恰好两行（内核 `IN_MOVED_FROM`/`IN_MOVED_TO` 天然成对，不做 cookie 拼接）；mv 出监听树只报 `rename_from`、mv 入只报 `rename_to`；`-e rename` 匹配 rename 家族 | [测试] 集成 s8（树内 mv / mv 出 / mv 入 + 新路径断言）、s9（子树移出后代误报回归）、s10（树内 rename 前缀改写 + 旧路径重建接管回归）；[实测] 一次 mv = 2 行 |
| FR-06 | glob 过滤：`-g` 按文件名匹配（`*` `?` `[...]` `[!...]`），可重复、任一命中即通过；无 `-g` 全通过；空模式参数拒绝（exit 2）。`--exclude` 反向过滤：语法与 `-g` 同源、可重复；放行 =（无 `-g` 或命中）且（无 `--exclude` 或未命中），exclude 优先于 include；只给 `--exclude` 等于"除了这些全都要"；仅影响上报不影响 watch 注册；`excludes=[]` 配置键替换语义随 `globs` 规则 | [测试] `wildmatch` 11 个单测（含边界固定测试）、集成 s2（屏障法负断言）、s23（exclude 三组合与上报层语义）、`errors_exit_path`、config 合并单测 |
| FR-06a | 脚本化退出：`--oneshot` 首个**通过过滤**的批次交付后走优雅停机（flush→join worker→exit 0），全部被过滤则继续等待；`--until-idle <SEC>` 每交付一批重置计时、静默满 SEC 秒优雅 exit 0，持续事件则存活；语义级实现不用内核 ONESHOT 位；配置键 `oneshot`(bool) / `until_idle`(整数秒 ≥1) | [测试] 集成 s24（oneshot 无信号有界退出；触发批 + 停机 drain 尾批，实测交付 2 事件——"恰一批"不成立）、s25（until-idle 时序 + 持续事件负断言）、`errors_exit_path` |
| FR-06b | 库 API：`fnotify::` 前缀公开面——`Config`（构造器风格，覆盖 CLI 全集）、`run` / `run_with_shutdown`、`Event`（批次条目）、`Error`（手写枚举，无新依赖）；嵌入为同步回调 sink，不暴露内部模块、不做 async；0.x 实验级承诺（design §9），`publish = false` 不变 | [测试] `tests/lib_api.rs`（公开面纯度：仅 `fnotify::` 前缀） |
| FR-07 | JSONL 输出：stdout 每行一条记录，`type` 判别字段先行——事件 `{"type":"event","ts":<RFC3339 UTC 毫秒>,"event":<名>,"path":<绝对路径>,"dir":<bool>}`；exec 生命周期 `exec_start`（pid、cmd）/`exec_end`（pid、cmd、exit、signal、timed_out、duration_ms），仅配置 `-x` 且未 `-q` 时输出，start/end 以 pid 关联；正确转义 `"` `\` 与控制字符（`\u00xx`）；非 UTF-8 路径 `to_string_lossy`，不 panic | [测试] `json_escape` 4 个、`jsonl_line` 形状、`format_ts` 3 个、exec 生命周期单测、集成 s6/s18；[实测] 冒烟 JSONL 输出 |
| FR-08 | exec 动作：`-x CMD` 对批内每条事件执行 `sh -c CMD`；占位符 `%p`（路径）/`%e`（事件名）；`%p` 以 POSIX 单引号包裹，**文件名内容不可注入 shell 语法**；exec 失败仅记 stderr 继续；`-q` 关闭 JSONL 输出；`--jobs N` 并发上限（默认 1 串行、池满背压），`--timeout SEC` 单 exec 超时（默认 0 不限，超龄者杀） | [测试] `shell_quote` 单测、集成 s6（端到端子进程，含恶意文件名逐字节断言）、s18（并发交错断言 + 超时杀与停机）、main 侧 `exec_pool_*` 3 个单测；[实测] 注入用例 `inj;touch-evil.log` 无法注入 |
| FR-09 | 信号与退出语义：SIGINT/SIGTERM 优雅停机（清空剩余事件、join worker、exit 0）；第二个信号立即退出（码 130，清空期间到达同样生效）；退出码约定 0=正常 / 1=运行错误 / 2=用法错误 | [测试] s5/s6 覆盖清空路径、s11（worker 崩溃 exit 1）、s12（用法错误 exit 2）；[实测] 信号行为为手工实测（无自动化，`kill -INT`/`-TERM`/双信号三例） |
| FR-10 | 监听目录被删除：stderr 一行告警（指明目录路径）、摘除该目录，其余目录继续监听；全部监听目录被删时优雅退出（exit 0） | [测试] 集成 s6（单根删除退出）、s15（逐目录摘除 + 摘光退出）；[实测] `rm -rf` 目录场景 |
| FR-11 | 离线构建与 RPM：`make dist` 产出含 vendored 依赖的源码包；Linux 上 `cargo build --release --offline`（rustc 1.82）与 `rpmbuild -bb` 全部通过 | [实测] 构建记录（design.md §11）；产物 `fnotify-0.1.0-1.<dist>.x86_64.rpm` |

## §3 非功能需求（NFR）

| # | 需求 | 验收方式与实测值 |
|---|---|---|
| NFR-01 | MSRV 1.82（RPM 构建链硬约束）；`rust-version` 声明 + Cargo.lock 入库 + 依赖精确 pin + 仓库根 `.cargo/config.toml` 启用 MSRV 感知解析（resolver fallback） | [实测] rustc 1.82 `cargo test --locked --offline` 全量三遍全绿；直接依赖 MSRV：inotify 1.70 / mio 1.71 / signal-hook 1.66 / toml 1.76 / libc 1.63（传递链 serde_spanned、toml_datetime、toml_parser 均 1.76，winnow 1.65），均低于 1.82 |
| NFR-02 | 依赖预算：直接依赖 5 个、Linux 目标全树 14 crate（toml 门面及其 5 个传递依赖 + libc），白名单外零引入 | [实测] `cargo tree --target x86_64-unknown-linux-gnu` 计数（口径：依赖 crate 不含 fnotify 自身；libc 由 inotify 传递依赖提升，不增全树规模） |
| NFR-03 | 资源基线：峰值 RSS ≤ 3MB；二进制 ≤ 1MB；持续写入场景输出行数 ≤ 原始事件数 1/4 | [实测] **release 二进制 908,128 B（887 KiB，≤1MB 达标）**；受控树 RSS 2.2MB（实测 2248KB，S1/S3 零增长）；输出 203 vs 1003 行（同口径复测 167 vs 1000，波动） |
| NFR-04 | 可靠性：引擎路径零 `unwrap`/`expect`/`panic!`；事件零丢失 | [实测] grep 复核（唯一 panic 在测试断言）；S1/S2 捕获率 100% |
| NFR-05 | 可测试性：全部测试在 Linux 跑通；结果三遍稳定 | [实测] 三遍全绿 |
| NFR-06 | 可观测性：stdout 仅含 JSONL（可管道/重定向）；告警与错误一律走 stderr | [实测] 基准脚本 `stderr_bytes=0`；错误路径输出核对 |

## §4 验收矩阵（出口条件）

| 验收项 | 通过标准 | 证据 |
|---|---|---|
| 单元+集成测试 | 全绿，三遍稳定 | 81 单测 + 27 集成 + 5 lib API（rustc 1.82，另 doctest 1） |
| 离线构建 | `cargo build --release --offline` 成功（rustc 1.82.0） | 实测；二进制 887KiB（908,128 B，≤1MB ✓） |
| inotify 冒烟 | 全生命周期事件正确（create/modify/rename_from/rename_to/remove/递归） | 冒烟输出记录（design.md §8、§12） |
| 信号/根删除 | INT/TERM 优雅 exit 0；双信号 130；根删除告警退出 | 实测（含双信号 130 复验） |
| RPM 交付 | `rpmbuild -bb` 出包，可查询元数据 | `fnotify-0.1.0-1.<dist>.x86_64.rpm`（`rpm -qip` License: MulanPSL-2.0） |
| 性能基线 | 达到 NFR-03 且与前身对比不留结构性劣势 | design.md §8 基线表 + 延迟实验 |

## §5 许可与规范

- 代码与文档采用 **木兰宽松许可证 v2（Mulan PSL v2）**：`LICENSE` 全文（中英双语），
  各源文件头部含标准声明；`Cargo.toml` 声明 `license = "MulanPSL-2.0"`，RPM spec 同。
- 文档体系对齐 suoji 惯例：`README.md`（使用）→ `docs/requirements.md`（本文，验收）→
  `docs/design.md`（设计与实测）→ `CHANGELOG.md`（Keep a Changelog）。

## §6 已知限制与后续方向

| 项 | 现状 | 后续方向 |
|---|---|---|
| 事件粒度 | 8 类（`metadata` 自 `IN_ATTRIB` 细分独立，s16 固化；`close_write` 掩码层 opt-in，s22 固化；open/close_nowrite 不订阅，准入判据见 design §3.4） | — |
| 首事件延迟 | 默认 = 去抖窗口（50ms；`--debounce 10` 实测 12ms）；`--flush immediate` 已落地（leading-edge 交付，实测 2–3ms 三连测，s13 固化） | — |
| exec 执行 | `--jobs` 小并发池（默认 1 串行，池满背压）+ `--timeout` 单 exec 超时杀（s18 固化） | 按事件类型差异化并发策略等 |
| 多目录删除 | 逐目录摘除、其余继续（s15 固化）；全部删光才整体退出 | — |
| 监听目录被 rename | mv 出监听树按删除同语义摘除（`IN_MOVE_SELF` 批末裁决，s17）；树内 rename 不动（嵌套根由父目录配对改写映射） | — |
| 短 flag 粘连 | 支持：布尔短 flag 可粘连（`-rq` = `-r -q`；含取值型短 flag 或未知字母仍 exit 2） | — |
| 配置 | CLI flag + `--config FILE`（TOML 配置文件，命令行优先，s20 固化） | — |
| webhook | `--webhook URL` 已落地（内网 http 零依赖手写，s19 固化） | https 引入 TLS 栈再评审 |
| CI | GitHub Actions 双工具链（stable + 1.82）fmt + 全量测试；本地 `make ci` 门禁合集 | — |
| spec URL | 已定为 https://github.com/jiachao2130/fnotify | — |
