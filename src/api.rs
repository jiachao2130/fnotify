// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! 库公开面：[`Config`] + [`run`] / [`run_with_shutdown`] + [`Error`]。
//!
//! 嵌入形态是同步回调 sink：引擎在专用 worker 线程上串行调用
//! `FnMut(&[Event])`，调用方要扇出（多个消费者、异步投递等）请在回调里
//! 自行 spawn——不在库内强加并发模型（design D14）。`run` 自行注册
//! SIGINT/SIGTERM 优雅停机；`run_with_shutdown` 把停机标志交给调用方。
//!
//! 内核 inotify 队列溢出（`IN_Q_OVERFLOW`）时，sink 会收到一条降级元记录：
//! `Event { event: "overflow", path: <空路径>, dir: false }`（`ts` 为溢出被
//! 引擎读到的时刻）。它不经过订阅掩码 / glob / 去抖三道闸——任何配置下都
//! 会交付，每次溢出各一条（风暴中多条属正常）；溢出后监听按磁盘现状重建，
//! 新事件照常上报。消费端据 `event == "overflow"` 给本轮统计打"已降级"
//! 标记：溢出窗口内的事件已被内核丢弃，该窗口计数不完整。
//!
//! 0.x 实验级 API：语义可随版本演进，破坏性变更在 CHANGELOG 明示（design §9）。

use crate::engine;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// 嵌入入口的配置：构造器风格，覆盖引擎输入全集（监听路径、递归、
/// glob / exclude 过滤、事件订阅、去抖、flush 模式、oneshot / until-idle、
/// ready-file 就绪信号）。exec / webhook / quiet 属于 CLI 侧动作组装，
/// 不入 Config——嵌入时 sink 由调用方全权构造。
#[derive(Debug)]
pub struct Config {
    paths: Vec<PathBuf>,
    recursive: bool,
    globs: Vec<String>,
    excludes: Vec<String>,
    event_mask: u32,
    debounce_ms: u64,
    flush_immediate: bool,
    oneshot: bool,
    until_idle: Option<Duration>,
    ready_file: Option<PathBuf>,
}

impl Config {
    /// 默认配置：监听 `.`、非递归、无过滤、默认事件集、50ms 去抖、
    /// trailing-edge flush、oneshot / until-idle 关、无就绪信号文件。
    pub fn new() -> Self {
        Config {
            paths: vec![PathBuf::from(".")],
            recursive: false,
            globs: Vec::new(),
            excludes: Vec::new(),
            event_mask: engine::DEFAULT_EVENTS,
            debounce_ms: 50,
            flush_immediate: false,
            oneshot: false,
            until_idle: None,
            ready_file: None,
        }
    }

    /// 监听目录（替换语义，默认 `.`）。路径在 run 时 canonicalize，
    /// 不存在返回 [`Error::Config`]（对应 CLI 的 exit 2）。
    pub fn paths<I, P>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        self.paths = paths
            .into_iter()
            .map(|p| p.as_ref().to_path_buf())
            .collect();
        self
    }

    /// 递归监听子目录（默认关）。
    pub fn recursive(mut self, on: bool) -> Self {
        self.recursive = on;
        self
    }

    /// 追加一个 `-g` 文件名通配模式（`*` `?` `[...]` `[!...]`，可重复）。
    pub fn glob(mut self, pattern: &str) -> Self {
        self.globs.push(pattern.to_string());
        self
    }

    /// 追加一个 `--exclude` 反向通配模式，命中即不上报（优先于 glob）。
    pub fn exclude(mut self, pattern: &str) -> Self {
        self.excludes.push(pattern.to_string());
        self
    }

    /// 事件订阅列表，取值域与 CLI `-e` 完全一致
    /// （`create,modify,remove,access,open,rename,metadata,close_write,other,all`），
    /// 复用同一解析函数；整体替换语义。未知名返回 [`Error::Config`]。
    pub fn events(mut self, list: &str) -> Result<Self, Error> {
        self.event_mask = crate::cli::parse_events(list).map_err(Error::Config)?;
        Ok(self)
    }

    /// 去抖窗口毫秒数；<10 按 10 计（与 CLI 同规则）。
    pub fn debounce_ms(mut self, ms: u64) -> Self {
        self.debounce_ms = ms.max(10);
        self
    }

    /// leading-edge 交付：每个（路径,事件名）首事件立即交付，窗口内抑制重复。
    pub fn flush_immediate(mut self, on: bool) -> Self {
        self.flush_immediate = on;
        self
    }

    /// 首个通过过滤的批次交付后优雅停机。
    pub fn oneshot(mut self, on: bool) -> Self {
        self.oneshot = on;
        self
    }

    /// 事件静默满 `secs` 秒后优雅退出（计时以批次交付为刷新点）。
    /// `secs` 为 0 会在 `run` / `run_with_shutdown` 时报 [`Error::Config`]
    /// ——与 CLI `--until-idle 0`、config `until_idle = 0` 同表同拒
    /// （0 的语义是"首个窗口即退"，并非"不启用"；不启用请省略调用）。
    pub fn until_idle(mut self, secs: u64) -> Self {
        self.until_idle = Some(Duration::from_secs(secs));
        self
    }

    /// 启动就绪信号：所有监听根（含递归子目录）watch 挂表完成、进入事件
    /// 循环前，在该路径创建空文件（一次性，此后不再写；退出时不删除，
    /// 生命周期由消费端管理）。创建失败仅 stderr 告警、不中断监控；
    /// 启动失败（路径不存在等）不创建——与 CLI `--ready-file` 同语义。
    pub fn ready_file(mut self, path: impl AsRef<Path>) -> Self {
        self.ready_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// 校验并转换为引擎输入：监听路径经 [`engine::check_roots`] 规范化
    /// （不存在 / 递归根嵌套即 Config 错，与 CLI 同规则同报错）；
    /// `until_idle = 0` 拒绝（正整数秒；不启用请保持 None）。
    fn into_options(self) -> Result<engine::Options, Error> {
        if matches!(self.until_idle, Some(d) if d.is_zero()) {
            return Err(Error::Config(crate::cli::until_idle_zero_msg("0")));
        }
        let paths = engine::check_roots(&self.paths, self.recursive).map_err(Error::Config)?;
        Ok(engine::Options {
            paths,
            recursive: self.recursive,
            globs: self.globs,
            excludes: self.excludes,
            event_mask: self.event_mask,
            debounce_ms: self.debounce_ms,
            flush_immediate: self.flush_immediate,
            oneshot: self.oneshot,
            until_idle: self.until_idle,
            ready_file: self.ready_file,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Config::new()
    }
}

/// 库入口的错误分类：Config = 配置/用法错误（CLI 对应 exit 2），
/// Runtime = 运行期错误（CLI 对应 exit 1）。
#[derive(Debug)]
pub enum Error {
    Config(String),
    Runtime(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(m) => write!(f, "{m}"),
            Error::Runtime(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {}

/// 生产入口：阻塞直到收到 SIGINT/SIGTERM 后优雅停机；信号语义与 CLI 完全一致
/// （第二个信号立即退出，码 130）。返回 Err 即运行期错误。
pub fn run(config: Config, sink: engine::Sink) -> Result<(), Error> {
    engine::run(config.into_options()?, sink).map_err(Error::Runtime)
}

/// 停机标志交给调用方的入口：置位 `shutdown` 后引擎有界清空、优雅收尾。
/// 适合嵌入已有运行时（daemon 框架、测试脚手架）的场景。
pub fn run_with_shutdown(
    config: Config,
    sink: engine::Sink,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Error> {
    engine::run_with_shutdown(config.into_options()?, sink, shutdown).map_err(Error::Runtime)
}
