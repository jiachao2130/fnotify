# fnotify

文件事件路由器（anotify 的重生）：监听目录 → 去抖聚合 → glob 过滤 → 输出动作。
仅支持 Linux（直连内核 inotify），同步线程模型，零 async，直接依赖 5 个 crate。
**中英双语** help 与错误消息（FNOTIFY_LANG=zh|en 可覆盖，默认跟随 LC_ALL > LC_MESSAGES > LANG）。

```text
事件循环线程：mio 轮询 inotify fd（非阻塞读尽）→ 去抖聚合 ──批次──▶ 动作 worker（串行）
主线程等 SIGINT/SIGTERM（每 200ms 轮询一次），收到信号后清空剩余事件、join worker、exit 0
```

## 用法

```console
$ fnotify -r /var/log                       # 递归监听，默认事件集，JSONL 输出
$ fnotify -g '*.log' -g '*.conf' /etc       # 只要 .log / .conf
$ fnotify -r -q -x 'logger -t fnotify -- %e %p'  # 静默 JSONL，事件转 syslog
$ fnotify -e access,modify --debounce 200 src
$ fnotify -r --flush immediate -g '*.log' /var/log  # 低延迟：首事件立即交付
$ fnotify -r --ready-file /run/fnotify/ready /var/log # 挂表完成即建就绪文件，消费端轮询到它再开跑（退出不删除）
$ fnotify --config fnotify.toml -g '*.log' .          # 配置作基线，命令行优先
```

完整选项见 `fnotify --help`。监听路径启动时 `canonicalize` 一次，不存在则报错退出（码 2）；
未知选项 / 非法 `-e` / 非法 `--debounce` 同样退出码 2。布尔短 flag 可粘连：`-rq` 等价 `-r -q`。

## 输出（JSONL，每行一条记录）

```json
{"type":"event","ts":"2026-10-16T09:05:00.123Z","event":"create","path":"/var/log/app/a.log","dir":false}
```

- `type`：记录判别字段，取值 `event` / `exec_start` / `exec_end` / `overflow`，下游按它分流。
- `ts`：RFC3339 UTC（毫秒精度，`Z` 结尾）。事件取首次被看到的时刻；exec 记录取发生时刻。
- 事件行：`event` / `path` / `dir` 语义见事件表；`dir` 取自内核 `IN_ISDIR` 标志
  （`rename_from` 的目标已不存在时仍按移动发生时的标记报告）。
- exec 生命周期（仅配置 `-x` 且未 `-q` 时输出）：
  - `exec_start`：`pid`（子进程 pid）、`cmd`（渲染后的完整命令行）；
  - `exec_end`：`pid`、`cmd`、`exit`（退出码，状态不可得为 `null`）、`signal`
    （终止信号，非信号终止为 `null`）、`timed_out`（是否被超时组杀）、`duration_ms`；
  - start/end 以 `pid` 关联。
- 队列溢出降级：内核 inotify 队列溢出时输出一条 `{"type":"overflow","ts":"..."}`
  （仅 `ts`，无 path/event/dir）——所在窗口的事件已被内核丢弃、统计已降级，
  任何订阅配置下都会输出。
- 非 UTF-8 路径用 `to_string_lossy`，不 panic。
- webhook 批次元素与 JSONL 行同构（溢出降级记录 `type:"overflow"` 同样进批次）。

## 事件表

| 事件名 | 内核事件来源 |
|---|---|
| `create` | `IN_CREATE` |
| `modify` | `IN_MODIFY`（内容写入） |
| `metadata` | `IN_ATTRIB`（元数据变更：chmod、chown、touch -m；默认事件集不含，需 `-e metadata` 指定） |
| `rename_from` | `IN_MOVED_FROM`（mv 的旧路径） |
| `rename_to` | `IN_MOVED_TO`（mv 的新路径） |
| `remove` | `IN_DELETE` |
| `access` | `IN_ACCESS`（默认事件集不含，需 `-e access` 指定） |
| `open` | `IN_OPEN`（默认事件集不含，需 `-e open` 指定。成功 open 即入队，天然覆盖 mmap 读；过计数见下文） |
| `close_write` | `IN_CLOSE_WRITE`（默认事件集不含，需 `-e close_write` 指定） |

`close_write` 的语义是"写事务结束"（打开写入的句柄关闭），不是"文件不会再变"。
典型场景是上传类操作判定收尾：`cp` / `scp` / `rsync` 这类工具写完即关句柄，
收到一条 `close_write` 即可视为该文件本轮写入完成；编辑器原地保存产生的是
`modify`，原子保存（写临时文件再 rename）产生的是 `rename_to`，都不对应
`close_write`。边界：daemon 常驻、句柄从不关闭的进程写的文件永不触发。

`open` 的语义是"成功 open(2)"，入队即覆盖 mmap 读——`access`（`IN_ACCESS`）
只覆盖 read(2) 类访问，clang 等编译器经 mmap 读源码/头文件时 `access` 整类
缺席，`open` 不受影响。过计数可接受：open+close 存在性探测、execve（exec
同样视为"参与了构建"）都会产生 `open`；open 失败不产生 `IN_OPEN`，无误报。
同一文件 read() 类访问会得到 `open` + `access` 两条（双报是设计行为，不做
事件合并，消费端按 path 去重）。

`-e rename` 一次性匹配 `rename_from` / `rename_to`。事件输出不做配对拼接：
一次 mv 固定产出旧路径 `rename_from` + 新路径 `rename_to` 两行；
mv 出监听树只有 `rename_from`，mv 入只有 `rename_to`。
目录映射维护用 cookie 配对区分树内 rename 与移出：树内 rename 后事件落新路径；
子树移出监听树后其全部后代 watch 被摘除，树外活动不会再以旧路径误报。
**默认事件集：`create,modify,rename,remove`**（`access` / `open` 不在默认集，需 `-e` 显式指定）。
`-e other` 为兼容保留可接受，但当前后端不产生（`CLOSE` / `CLOSE_NOWRITE`
等不投递；`MOVE_SELF` 常驻内核掩码、仅内部用于目录改名改写，不作为事件上报）。

## glob 过滤

`-g` 按文件名（路径最后一段）匹配，可重复、任一命中即通过；无 `-g` 全通过。
语法：`*`（不跨 `/`）、`?`、`[abc]`、`[a-z]`、`[!abc]`（取反，`^` 亦可）。
不做 shell 隐含规则：`.` 开头的文件名默认可被 `*` 匹配；未闭合的 `[` 按字面量。
非 UTF-8 文件名先经 `to_string_lossy`（含 U+FFFD 替换字符）：含字面字符的模式永不命中
这类文件名，`*`/`?` 等通配仍可通过。

`--exclude <PATTERN>` 为反向过滤，语法与 `-g` 同源，可重复。放行规则：

- 无 `-g`（或 `-g` 命中）**且**（无 `--exclude` 或 `--exclude` 未命中）才上报；
- `--exclude` 优先于 `-g`：同一文件名同时命中两者时被排除；
- 只有 `--exclude`、没有 `-g` = "除了这些全都要"。

`--exclude` 只影响事件上报，不影响 watch 注册（被排除的文件/目录仍被监听，
只是不上报）。

## 去抖语义

事件循环以 `debounce` 为窗口轮询 inotify fd；窗口内无新事件才把聚合批次交给 worker。
同一批内按 `(路径, 事件名)` 去重——每批每路径每事件名只触发一次。
默认窗口 50ms，`--debounce` 小于 10 时按 10 计。

**批次时长封顶**：事件持续到达（间隔小于窗口，如繁忙日志目录）时，
批次不会无限累积——时长满一个窗口即交付，即繁忙流下批次延迟以 `--debounce` 为界。
停机清空同样有界（总时长 ≤ 2×窗口）。

**`--flush immediate`**（leading-edge 交付）：每个（路径,事件名）的首事件立即交付，
不经静默窗口等待，端到端延迟约为管道开销（数毫秒）；其后一个去抖窗口内的同键重复被抑制。
`--debounce` 在 immediate 模式下控制的是抑制间隔而非聚合窗口。默认 `debounce` 行为不变。

## 停机与退出语义

- `SIGINT` / `SIGTERM`：优雅停机（清空剩余事件、join worker、exit 0）；
  **第二个信号**到达则放弃优雅停机，立即退出（码 130），不会只能 SIGKILL——
  清空期间到达的第二个信号同样生效（引擎收尾 join 前还会最后再检查一次信号）。
- **监听目录被删除**：内核补发 `IN_IGNORED`，fnotify 在 stderr 告警一行（指明哪个目录）
  并摘除该目录，其余目录继续监听；全部监听目录被删才优雅退出（exit 0）。
  监听目录被 **mv 出监听树**（`IN_MOVE_SELF` 探测）按删除同语义处理；树内 rename 不受影响。
- **动作 worker 异常退出**（sink panic 等）：flush 发送失败即停止监听，以 exit 1 结束。
- **`--oneshot`**：首个通过过滤的批次交付后即优雅停机（exit 0），适合"等到一次变化
  就走"的脚本；全部事件被过滤掉则继续等待。
- **`--until-idle <SEC>`**：每交付一批重置计时，事件静默满 SEC 秒优雅退出（exit 0），
  事件持续则一直运行。

## exec 占位符与 shell 注入

`-x 'CMD'` 对批次内每条事件执行 `sh -c CMD`（stdout/stderr 继承，失败记 stderr 一行继续）：

- `%p` → 事件路径，**以 POSIX 单引号包裹**（`it's` → `'it'\''s'`），文件名内容无法注入 shell 语法；
- `%e` → 事件名（本程序自有常量，安全）；
- 孤立的 `%` 原样保留。

注意：命令模板本身仍由你信任的 `sh` 解释，只替换占位符，不解析模板语义。
exec 失败（非零退出 / 无法启动）不会中断主循环。

**并发与超时**：`--jobs N` 控制 exec 最大并发进程数（默认 1，即串行有序）；
池满时新事件背压排队。`--timeout SEC` 给单个 exec 设超时（默认 0 不限制），
超龄进程按进程组杀（含 `sh -c` 派生的子进程）；超时由后台线程按时钟驱动，
事件流静默期间同样生效。优雅停机时会等在跑进程结束（超时杀仍生效）。

## webhook

`--webhook URL` 把每个去抖批次以 JSON 数组（元素与 JSONL 行同构）POST 到远端：

```console
$ fnotify -r -q --webhook http://127.0.0.1:8080/hook /var/log
```

- 仅支持 `http://`：客户端为手写最小 HTTP/1.1（`Connection: close`，读状态行判成败），
  不引入 TLS 依赖；`https://` 需要 TLS 栈，引入时再评审（design §6）。
- 推送在动作 worker 上同步执行，整体超时上限 10s；失败（连不上、非 2xx）记
  stderr 一行继续，不打断事件流，也不重试——需要至少一次投递的场合请走 exec 自行接力。

## 配置文件

`--config FILE` 从 TOML 文件加载选项，键与命令行选项同名（`paths` 为字符串数组，
对应位置参数）：

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
ready_file = "/run/fnotify/ready"  # 就绪信号文件；不启用请省略该键
quiet = false
```

- 优先级：默认值 < 配置文件 < 命令行。取值型选项（`--debounce`、`--webhook` 等）
  直接被命令行覆盖；`-g` / `--exclude` 与位置参数为替换语义（命令行一旦出现，
  配置里的 `globs` / `excludes` / `paths` 即被替换）；布尔开关只增不减——
  命令行无法关掉配置里已开启的项。
- 未知键、类型错误、空串取值一律启动即报错（退出码 2），不静默吞掉拼错的键。

## 依赖理由

| 依赖 | 版本 | 为什么不可手写 / 为什么需要 |
|---|---|---|
| `inotify` | `=0.11.5` | 内核事件源的封装（fd 生命周期、事件解析）。递归注册（add-before-scan，风暴创建不丢目录）、wd 映射、新建子目录接管等状态维护集中在 `src/watch.rs` 一个模块，是旧项目 anotify 垮掉之处的重写，配套 s7/s8/s9/s28 集成测试固化 |
| `mio` | `1.2`（关默认特性，仅 `os-poll`/`os-ext`） | epoll 轮询 inotify fd，替代 notify 自带的 watcher 线程；fd 非阻塞 + WouldBlock 读尽即可 |
| `signal-hook` | `0.4` | 信号安全处理（SIGINT/SIGTERM 优雅停机），比裸 `signal()` 注册可靠 |
| `toml` | `~1.0`（仅 `std`/`parse`/`serde` 特性） | TOML 语法解析（转义、多行字符串、嵌套表）不值得手写；DOM API 在 serde 特性下，连带 serde_core 仅作传递依赖进构建，fnotify 自身代码零 serde |

glob 匹配、JSON 转义、参数解析、目录树遍历与 wd 管理均为手写（`src/wildmatch.rs`、
`src/engine.rs`、`src/cli.rs`、`src/watch.rs`），不引入 regex / clap。

## 库 API（0.x 实验级）

fnotify 同时以库形态可嵌入：`fnotify::Config` 覆盖全部 CLI 选项，`fnotify::run(config, sink)` /
`fnotify::run_with_shutdown(config, sink, flag)` 以同步回调接收 `fnotify::Event` 批次
（扇出由调用方自行 spawn）。语义与稳定性承诺见 design §9；`Cargo.toml` 维持
`publish = false`，暂不发布 crates.io。

## 构建

仓库内 `Makefile`：

```sh
make check    # cargo fmt --check && cargo clippy --all-targets -D warnings && cargo test（Linux；macOS 不能编译本项目）
make ci       # 门禁合集 = check + msrv（push 前在 Linux 机器上过一遍）
make vendor   # cargo vendor vendor/ + 生成 build/.cargo/config.toml（offline 配置，不污染仓库根）
make dist     # 组装 dist/fnotify-0.1.0.tar.gz（src + tests + Cargo.toml + Cargo.lock + vendor/ + .cargo/ + spec + completions）
make msrv     # cargo +<地板工具链> check --locked（Linux 机器；地板见 Cargo.toml 的 rust-version）
```

在 Linux 构建机上出 RPM：一键 `bash scripts/make-srpm.sh`（组装含 vendored
依赖的源码包并直接 `rpmbuild -bs` 出 SRPM，全程无网；产物路径在末尾输出）。
手动流程等价于 `make dist` 后把 tarball 放 `~/SOURCES/`、仓库根的
`fnotify.spec` 放 `~/SPECS/`（topdir 实测跟随 cwd：rpm 内置默认取 PWD，
无 `~/.rpmmacros` 覆盖——构建前先 `rpm --eval '%_topdir'` 确认落点，固定 cwd
后执行 rpmbuild），`rpmbuild -bb`；`%build` 为 `cargo build --release`
（vendored 依赖随源码包提供，全程无网；spec 声明 `BuildRequires: rust`）。已在
地板工具链下实测出包：
`fnotify-0.1.0-1.<dist>.x86_64.rpm`（dist 标签随构建系统）。RPM 同时安装
bash/zsh 补全；手动安装可 `source completions/fnotify.bash`，或把
`completions/_fnotify` 加入 zsh 的 `$fpath`。

CI：`.github/workflows/ci.yml` 已就绪（push / PR 触发，stable + 地板工具链双档
fmt + test），仓库地址 https://github.com/jiachao2130/fnotify。

## MSRV 策略

- 最低工具链版本由 `Cargo.toml` 的 `rust-version` 声明（单一事实源），main 为
  edition 2024；`msrv-1.82` 分支保留 1.82 兼容基线（RPM 构建链锚点）。
- `Cargo.lock` 入库、`inotify` 精确 pin（`mio` 锁 minor）：事件分类依赖内核语义的封装行为，
  锁死保证各构建机行为一致，也让 vendored 源码包可复现。
- 升级 MSRV 需伴随一次全平台测试通过并更新本文件。

## 暂未实现的计划

- `--format` 自定义输出模板；
- 事件批上限与背压策略（当前无界队列，追加式场景足够）；
- systemd 单元与 `Type=notify` 集成（当前保持无守护进程假设）；
- inotify 专有调优（`/proc/sys/fs/inotify/*` 与队列深度的关系梳理）。

## 文档

- [docs/usage.md](docs/usage.md) —— 使用说明：安装、选项速查、systemd 部署与常见问题
- [docs/requirements.md](docs/requirements.md) —— 需求（FR/NFR）与验收矩阵
- [docs/design.md](docs/design.md) —— 设计方案、决策表与实测基线
- [CHANGELOG.md](CHANGELOG.md) —— 变更记录

## License

木兰宽松许可证，第2版（Mulan PSL v2），见 [LICENSE](LICENSE)。
各源文件头部含标准声明；`Cargo.toml` 与 RPM spec 声明 `MulanPSL-2.0`。
