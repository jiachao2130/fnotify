# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-16

初始发布。

### Added

- inotify 递归监听：启动前整树注册、运行中新建子目录动态接管、子树移出整棵
  摘除、wd↔路径映射维护；单根删除/移出仅摘除该根，全部根失去才退出。
- 事件分类：create / modify / remove / access / rename / metadata /
  close_write（掩码层 opt-in，"写事务结束"语义）/ rename_from / rename_to
  （一次 mv 恰好两行，不做 cookie 拼接）/ open（IN_OPEN，opt-in，`-e open`；
  覆盖 mmap 类文件访问，`access` 只覆盖 read(2)；read() 类访问 open+access
  双报为设计行为，消费端按 path 去重）；默认集
  create,modify,rename,remove。
- 去抖聚合：静默窗口（`--debounce`，默认 50ms）内按 `(路径,事件名)` 去重，
  繁忙流批次时长以窗口封顶；`--flush immediate` 首事件直通、窗口内抑制重复
  （实测 2–3ms 交付）。
- glob 过滤：`-g` 文件名通配（`*` `?` `[...]` `[!...]`，可重复），
  `--exclude` 反向过滤（优先于 include）；仅影响上报不影响注册。
- 动作管线：JSONL stdout（type 判别 + RFC3339 UTC 时间戳）、`-x` 按事件
  执行命令（`%p`/`%e` 模板、并发池 `--jobs`、超时组杀含孙进程、时钟驱动回收）、
  `--webhook` 手写 HTTP/1.1 POST（零网络依赖，连接 10s 封顶；URL 全段控制
  字符校验，请求行 CRLF 注入拒绝，拒绝消息回显按转义形态显示）。
- 脚本化运行：`--oneshot` 首批交付即退、`--until-idle SEC` 静默期满退
  （计时以批次交付为刷新点）；停机收尾有界（`drain_bounded`，持续写入流
  下照常退出）；`--ready-file <path>` 启动就绪信号（全部根 watch 挂表
  完成后创建，启动失败不创建，停机不删除；CLI / TOML `ready_file` 键 /
  库 API `Config::ready_file()` 三入口齐备，供消费端替换 magic sleep）。
- `--` 终结符：其后的参数一律按监听路径处理（含 `-x`、`--foo` 字面量），
  支持监听以 `-` 开头的目录；`--config` 预扫描同口径（`--` 之后的
  `--config=...` 按路径处理，不被加载剥离）。
- TOML 配置（`--config`，命令行优先）与中英双语 help/错误消息
  （`FNOTIFY_LANG` > LC_ALL > LC_MESSAGES > LANG）。
- 库 API（0.x 实验级）：`Config` / `run` / `run_with_shutdown` / `Event` /
  `Error`，同步回调 sink；内部模块 `#[doc(hidden)]`；校验与 CLI 同表同拒。
- 优雅停机：SIGINT/SIGTERM 有界清空，第二信号立即 exit 130；RPM 交付
  （实测构建链路，`%build` 全程离线）。
- 健壮性：engine→worker 有界通道（积压 64 批，背压直至 Q_OVERFLOW 重建——
  内核丢事件、watch 全量重注册）；递归监听遇 EACCES 子树告警一行、
  其余子树照常；exec 超时不留孤儿进程组；非 UTF-8 文件名 `%p` 字节级直通
  （命令拿到真路径，展示面 lossy）；`IN_Q_OVERFLOW` 内核队列溢出时输出
  `{"type":"overflow","ts":...}` JSONL 降级记录（绕过订阅掩码/glob/去抖，
  任何配置下都交付，库 API sink 同步收到），把"高负载下队列打满静默丢事件"
  变成消费方可显式感知的降级标记。

### Fixed

- **去抖合并的事件时间戳取首次触发，后触发被吞（正确性缺陷）**：同一
  文件同一事件名在一条去抖窗口内多次触发时合并为一条，交付 ts 原为
  **首次**触发时刻——按时间过滤的消费端会把窗口内较晚发生的读取错判为
  更早。改为合并取**末次**触发（trailing-edge："该事件代表最近一次活动"
  与静默窗口语义自洽）；同窗多次触发仍恰交付一条（去重承诺不变）；
  immediate 模式为 leading-edge（首事件已即时发走，不回写）。
  单测 aggregator_merge_takes_trailing_timestamp + 集成 s35 红绿实测。
- **sw_64 收 SIGTERM 即 abort（申威平台修复）**：vendored libc 0.2.190
  对 sw_64 无 sigaction 位值覆盖，SA_* 落到 asm-generic 值，SA_SIGINFO
  位从未到达内核（sw_64 真实 ABI：SA_SIGINFO=0x40、SA_RESTART=0x2），
  内核按单参约定调用三参 handler、siginfo 为 NULL，signal-hook-registry
  防御性 abort。Patch0 新增 sw64/bits/sigaction.rs（SA_*/SIG_BLOCK 全套
  + struct 布局，对齐本机 glibc 头）。sw_64 真机验证：冒烟 TERM 退出
  exit 0（此前 134 abort），全量测试套件首次在 sw_64 通过。上游
  rust-libc 补齐 sw_64 sigaction 后本段可撤。
- **创建风暴下子树永久失聪（正确性缺陷）**：接管/启动注册采用
  add-before-scan 顺序不变式——对任意目录先挂 watch 再枚举子项，快照
  与挂表之间的窗口内新建由该 watch 的 CREATE 兜住，杜绝"整棵子树从此
  无事件"（s28/s32 回归，SIGSTOP 放大器 50 轮零失聪）。
- 扫描期掩码与升级时机：readdir 的 getdents/opendir 会在刚挂的 watch 上
  自触发 ACCESS/OPEN/ATTRIB，若以前序升级会以 2×N 自产事件灌爆 16384
  内核队列形成 overflow→rescan→再灌爆自激（20000 目录实测）。扫描期
  掩码摘除 ACCESS|OPEN|ATTRIB，子树 DFS 完成后后序升级回完整掩码；
  readdir 失败分支同样升级（纵深防御）。代价：接管窗口内目录自身的
  access/open/metadata 事件在内核层不排队（文件事件不受影响，
  见 design §3.4 取舍记录）。
- 非递归模式 Q_OVERFLOW 重建只挂根目录本身，不下钻子树（防 watch 配额
  爆炸）。
- 迟到的 IN_IGNORED（旧 wd）不盲删同路径重建后的新映射表项。

### Changed

- 多事件订阅文档明确：逗号分隔（`-e access,open`）；重复 `-e` 为整体
  替换（last-wins），`-e access -e open` 只剩 open。

### Quality

- 129 例测试（87 单测 + 35 集成 + 6 lib API + 1 doctest），rustc 1.82
  离线 `--locked` 三遍全绿；s26（oneshot 持续写入流有界退出）的回归防线
  经红绿实测坐实（旧式无界收尾实现下必挂）。
- 资源基线实测：二进制 912,224 B（891 KiB，
  ≤1MB；rustc 1.82 构建）；受控树 RSS 2.2MB（≤3MB）；持续写输出 ≤ 原始 1/4。
- 引擎路径零 `unwrap`/`expect`/`panic!`（lint 强制）；错误一律 stderr，
  stdout 仅 JSONL。
