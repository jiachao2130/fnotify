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
  （一次 mv 恰好两行，不做 cookie 拼接）；默认集 create,modify,rename,remove。
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
  下照常退出）。
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
  （命令拿到真路径，展示面 lossy）。

### Quality

- 114 例测试（81 单测 + 27 集成 + 5 lib API + 1 doctest），rustc 1.82
  离线 `--locked` 三遍全绿；s26（oneshot 持续写入流有界退出）的回归防线
  经红绿实测坐实（旧实现下必挂）。
- 资源基线实测：二进制 908,128 B（887 KiB，≤1MB）；受控树 RSS 2.2MB（≤3MB）；
  持续写输出 ≤ 原始 1/4。
- 引擎路径零 `unwrap`/`expect`/`panic!`（lint 强制）；错误一律 stderr，
  stdout 仅 JSONL。
