# fnotify 使用说明

面向部署与日常使用的操作文档。行为语义细节见 [README](../README.md) 与
[design](design.md)，本文不重复论证，只讲怎么用。

## 1. 安装

**RPM（推荐）**：

```console
# rpm -i fnotify-0.1.0-1.<dist>.x86_64.rpm
# fnotify --version
fnotify 0.1.0
```

装好后只有二进制 `/usr/bin/fnotify` 与 bash/zsh 补全文件，无配置文件、
无服务脚本、无依赖服务。

**二进制**：从 dist 源码包（`make dist` 产物）解出即可构建，依赖已 vendor，
全程离线：

```console
$ tar -xzf fnotify-0.1.0.tar.gz && cd fnotify-0.1.0
$ cargo build --release --offline
$ ./target/release/fnotify --version
```

**运行环境**：Linux（事件源是内核 inotify，其他平台不支持）。`fnotify` 以一个
前台进程运行，本身不守护化——作为服务运行请交给 systemd（见 §9）。

## 2. 快速开始

```console
$ fnotify -r /var/log                          # 递归监听，默认事件，JSONL 打到终端
$ fnotify -g '*.log' -g '*.conf' /etc          # 只关心 .log / .conf
$ fnotify -r -q -x 'logger -t fnotify -- %e %p' . # 静默 JSONL，事件转 syslog
$ fnotify -e access,modify --debounce 200 src  # 订阅访问/修改，200ms 去抖
$ fnotify -r --flush immediate -g '*.log' /var/log  # 首事件立即交付（毫秒级延迟）
```

程序前台运行，`Ctrl-C`（SIGINT）优雅停机，退出码 0。所有用法错误（路径不存在、
未知选项、非法取值）启动即报错并以退出码 2 结束，不会带病运行。

## 3. 选项速查

| 选项 | 取值 | 默认 | 说明 |
|---|---|---|---|
| `PATH`（位置参数） | 目录，可多个 | `.` | 监听目录；启动时规范化为绝对路径 |
| `-r, --recursive` | 开关 | 关 | 递归监听子目录（含运行中新建的） |
| `-g, --glob` | 模式，可重复 | 无（全通过） | 按文件名过滤，任一命中即通过 |
| `--exclude` | 模式，可重复 | 无 | 反向过滤：命中即不上报；优先于 `-g` |
| `-e, --events` | 逗号分隔列表 | `create,modify,rename,remove` | 事件子集；`rename` 同时匹配 from/to；`all` 全选 |
| `--debounce` | 毫秒 | 50（<10 按 10 计） | 去抖窗口 / immediate 模式的抑制间隔 |
| `--flush` | `debounce` / `immediate` | `debounce` | immediate：每（路径,事件）首事件立即交付 |
| `-x, --exec` | 命令模板 | 无 | 每事件经 `sh -c` 执行；`%p` 路径、`%e` 事件名 |
| `--jobs` | 正整数 | 1 | exec 最大并发；池满背压排队 |
| `--timeout` | 秒 | 0（不限制） | 单 exec 超时，按进程组杀，时钟驱动（静默期同样生效） |
| `--webhook` | `http://` URL | 无 | 每批次 JSON 数组 POST，整体超时 10s，失败不重试 |
| `--oneshot` | 开关 | 关 | 首个通过过滤的批次交付后优雅退出（exit 0） |
| `--until-idle <SEC>` | 秒 | 省略 = 不启用 | 每交付一批重置计时；静默满 SEC 秒优雅退出 |
| `--config` | TOML 文件 | 无 | 配置作基线，命令行优先 |
| `-q, --quiet` | 开关 | 关 | 关闭 JSONL stdout 输出 |
| `-h, --help` / `-V, --version` | — | — | 帮助 / 版本 |

布尔短 flag 可粘连：`-rq` 等价 `-r -q`。`--oneshot` 与 `--until-idle` 同时给出时
先到的条件生效（交付首批即 oneshot 退出；未交付任何批则 until-idle 计时）。

**glob 与反向过滤**：`-g` 与 `--exclude` 语法同源（`*` `?` `[...]` `[!...]`），
都按文件名匹配、可重复。放行规则 =（无 `-g` 或 `-g` 命中）且（无 `--exclude` 或
未命中）；两者冲突时 `--exclude` 优先。只给 `--exclude` 等于"除了这些全都要"。
`--exclude` 只影响上报，不影响监听注册。

## 4. 输出

默认每事件一行 JSONL 到 stdout（`-q` 关闭全部 stdout 记录，含 exec 生命周期）：

```json
{"type":"event","ts":"2026-10-16T09:05:00.123Z","event":"create","path":"/var/log/app/a.log","dir":false}
```

- `type`：记录判别字段——`event`（文件事件）/ `exec_start` / `exec_end`。
- `ts`：RFC3339 UTC，毫秒精度；事件取首次被看到时刻。
- `event`：`create` / `modify` / `metadata` / `rename_from` / `rename_to` / `remove` / `access` / `close_write`。
- `path`：绝对路径。
- `dir`：是否目录（内核 `IN_ISDIR`）。

一次 `mv` 固定产出两行：`rename_from`（旧路径）+ `rename_to`（新路径）。
mv 出监听树只有 `rename_from`，mv 入只有 `rename_to`——程序不做配对拼接，
下游自行关联。同一批内按（路径, 事件名）去重，每批每键只触发一次。

**exec 生命周期**（仅配置 `-x` 且未 `-q` 时输出，与事件同流）：

```json
{"type":"exec_start","ts":"...","pid":1234,"cmd":"logger -t fnotify -- create /x"}
{"type":"exec_end","ts":"...","pid":1234,"cmd":"...","exit":0,"signal":null,"timed_out":false,"duration_ms":12}
```

start/end 以 `pid` 关联；`exit` 为退出码（状态不可得为 `null`），`signal` 为终止
信号（非信号终止为 `null`），`timed_out` 标记是否被超时组杀。spawn 失败不发
生命周期记录（也没有 pid 可关联），仍走 stderr 告警。

## 5. 事件订阅要点

- 默认集 `create,modify,rename,remove` 已覆盖大多数"文件变了"场景。
- `metadata`（`IN_ATTRIB`：chmod、chown、`touch -m`）与 `access` 不在默认集，
  需要时 `-e` 显式加；`-e` 是整体替换语义（如 `-e modify,metadata`）。
- 权限/时间戳类监控（如"谁改了权限"）用 `-e metadata`；
  审计类场景（"谁读了文件"）用 `-e access`——注意 access 事件量大。
- `close_write`（写句柄关闭，需 `-e close_write`）表示"写事务结束"，不是
  "文件不会再变"。适用场景是上传类收尾判定：`cp` / `scp` / `rsync` 写完即关
  句柄，收到一条即可视为本轮写入完成；daemon 常驻、句柄从不关闭的进程写的
  文件不会触发。编辑器原地保存是 `modify`，原子保存是 `rename_to`，都不是它。

## 6. 去抖与延迟

默认 `debounce` 模式（trailing-edge）：窗口内静默才把批次交出，稳态延迟约
一个窗口（默认 50ms）；繁忙流不会无限累积——批次时长满一个窗口即交付。

低延迟场景用 `--flush immediate`（leading-edge）：每个（路径,事件）的首事件
立即交付，实测端到端 2–3ms；其后一个去抖窗口内的同键重复被抑制。
`--debounce` 在该模式下控制抑制间隔。

选择口径：下游做索引/同步、要的是"最终一致"用默认；做触发/联动、要的是
"尽快响应"用 immediate。

## 7. 动作

### exec（`-x`）

每事件执行一次 `sh -c CMD`，占位符 `%p`（路径，POSIX 单引号包裹，文件名内容
无法注入 shell 语法）与 `%e`（事件名）。模板本身由你信任的 `sh` 解释。

- 每次运行与退出都以 `exec_start` / `exec_end` 记录进 stdout 结构化流（未 `-q` 时，
  字段见 §4）；非零退出另记 stderr 一行给人看，启动失败（spawn 不出进程）只记 stderr；
- `--jobs` 控制并发（默认 1 串行有序）；池满时新事件背压排队；
- `--timeout` 超龄进程按进程组杀（含 `sh -c` 派生的子进程），停机时同样生效。

典型用法：

```console
$ fnotify -r -q -x 'logger -t fnotify -- %e %p' /var/log        # 转 syslog
$ fnotify -q -g '*.conf' -x 'nginx -t && systemctl reload nginx' /etc/nginx
$ fnotify -r -q -g '*.done' -x 'rsync -a --remove-source-files %p /srv/inbox/' ./upload
```

需要可靠投递时走 exec 自行接力（webhook 不重试）。

### webhook（`--webhook`）

每个去抖批次以 JSON 数组 POST 到 `http://` 端点（元素与 JSONL 行同构）。
仅支持 http——客户端是手写最小 HTTP/1.1，不带 TLS；推送在动作 worker 上
同步执行，整体超时上限 10s，失败仅记 stderr，不重试。

## 8. 配置文件

固定部署建议写 TOML 配置，命令行只做临时覆盖：

```toml
paths = ["/var/log", "/etc"]
recursive = true
globs = ["*.log", "*.conf"]
excludes = ["*.tmp"]        # 反向过滤：命中即不上报
events = "create,modify,rename,remove"
debounce = 50
flush = "debounce"          # 或 "immediate"
exec = "logger -t fnotify -- %e %p"
jobs = 1
timeout = 0                 # 秒；0 = 不限制
webhook = "http://127.0.0.1:8080/hook"
oneshot = false             # true = 交付首批后退出
until_idle = 3              # 秒；不启用请省略该键，0 与负数报错
quiet = false
```

优先级：默认值 < 配置 < 命令行。取值型选项命令行直接覆盖；`-g` / `--exclude`
与位置参数是替换语义；布尔开关只增不减。未知键、类型错误启动即报错（退出码 2）。

## 9. 作为服务运行（systemd）

程序按前台进程设计，systemd 单元很薄：

```ini
# /etc/systemd/system/fnotify.service
[Unit]
Description=fnotify file event router
Documentation=file:/usr/share/doc/fnotify/README.md
After=local-fs.target

[Service]
Type=simple
ExecStart=/usr/bin/fnotify --config /etc/fnotify/fnotify.toml
Restart=on-failure
RestartSec=2
# 崩溃/被杀后按退出码处理：0（正常/信号/根删光）不重拉，1/2 重拉前先限次
StartLimitIntervalSec=60
StartLimitBurst=5

[Install]
WantedBy=multi-user.target
```

```console
# systemctl daemon-reload && systemctl enable --now fnotify
# journalctl -u fnotify -f          # stdout（JSONL）与 stderr（告警）都进 journal
```

要点：

- **JSONL 是主输出**，journal 里一行一条事件，直接可用 `journalctl -u fnotify -o
  cat` 抽数据；要落文件就在单元里加 `StandardOutput=append:/var/log/fnotify/events.jsonl`
  （systemd ≥ 240）并把告警留在 journal。
- 优雅停机由 systemd 发 SIGTERM 触发；`Restart=on-failure` 只对非零退出生效，
  正常停机（exit 0）不会触发重拉。
- 监听目录被删光时 fnotify 自行退出（码 0）——若这是异常状态，用 `Restart=always`
  并在单元里加 `ExecStartPre=/bin/mkdir -p …` 重建目录。

## 10. 运行行为速记

| 情形 | 行为 | 退出码 |
|---|---|---|
| SIGINT / SIGTERM | 清空剩余事件、等在跑 exec 结束 | 0 |
| 第二个信号 | 放弃优雅停机立即走 | 130 |
| `--oneshot` 交付首批后 | 自行优雅停机（无需信号） | 0 |
| `--until-idle SEC` 静默期满 | 自行优雅停机（无需信号） | 0 |
| 监听目录被删除 | 告警并摘除该目录，其余继续；删光才退 | 0 |
| 监听目录被 mv 出树 | 同删除语义（`IN_MOVE_SELF`） | 0 |
| 动作 worker 异常 | 停止监听 | 1 |
| 用法错误（路径不存在、未知选项等） | 启动即拒 | 2 |

## 11. 脚本化用法

两个为脚本设计的退出开关，都不需要信号即可自行结束（exit 0）：

```console
$ fnotify --oneshot -g '*.done' ./inbox   # 等到第一个 .done 文件出现并交付后退出
$ fnotify --until-idle 5 -r ./inbox       # 事件静默满 5 秒后退出，持续活动则一直运行
```

- `--oneshot` 判的是**通过过滤**的批次：全部被 glob / 事件掩码过滤掉的事件
  不触发退出，继续等待。
- `--until-idle` 的计时以**批次交付**为刷新点，不是以事件发生为刷新点；去抖
  窗口内的连续事件算一批。静默期满的退出判定也以交付为界——最后一批送达后
  还需静默满 SEC 秒，实际退出时刻 ≈ 末批交付 + SEC + 至多一个去抖窗口。
- 两者同时给出时，先满足的条件生效。与 `--config` 搭配的对应键为
  `oneshot = true` 与 `until_idle = 5`。
- 典型搭配：`fnotify --oneshot -e close_write -g '*.zip' ./upload`——等到一个
  上传（写完关闭）即退出，交给脚本做后续处理。

## 12. 常见问题

**事件没来 / 监听没反应？**
先确认路径：监听目录启动时一次性规范化，之后只认这条绝对路径（树内 rename
会自动跟随，但根本身被替换不算）。非递归模式下子目录里的事件收不到——加 `-r`。

**大量事件下丢事件？**
inotify 有内核队列上限（`/proc/sys/fs/inotify/max_queued_events`，默认常见
16k）。高频场景（批量解压、编译）下游若发现事件缺失，先查这两个内核参数：
`max_queued_events`（瞬时队列深度）与 `max_user_watches`（监听注册数上限，
递归监听大目录树时容易顶到）。fnotify 自身去抖聚合已把输出缩减数倍，
但源头突刺仍受内核限制。

**rename 为什么是两行？**
不做配对是设计取舍：mv 出树 / 入树时配对天然残缺，强行拼接反而丢信息。
下游按（路径相邻、时间相邻）关联即可；监控删除类场景直接订阅 `remove`。

**非 UTF-8 文件名？**
路径经 `to_string_lossy` 输出（含 U+FFFD 替换字符），不会 panic；
glob 模式里的字面字符对这类文件名不命中，`*`/`?` 通配仍可通过。

**中文/英文消息？**
help 与错误消息中英双语，默认跟随 `LC_ALL > LC_MESSAGES > LANG`，
`FNOTIFY_LANG=zh|en` 可强制指定。
