// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! fnotify —— Linux 优先的文件事件路由器：监听 → 去抖 → glob 过滤 → JSONL / 执行命令。
//!
//! 模块划分（内部实现，经 `#[doc(hidden)]` 公开仅供二进制 crate 复用，
//! 不构成承诺的库 API）：
//! - [`locale`]：中英双语消息（help/usage 横幅 + 错误消息模板）
//! - [`wildmatch`]：fnmatch 风格的文件名通配匹配（纯函数，无依赖）
//! - [`watch`]：inotify 监听封装（递归注册、子目录动态接管、wd 映射维护）
//! - [`engine`]：事件循环 + 去抖聚合 + 过滤 + 动作管线（同步线程模型）
//! - [`cli`]：手写参数解析
//! - [`config`]：TOML 配置文件加载（--config，命令行优先）
//! - [`hook`]：webhook 推送（手写最小 HTTP/1.1 客户端，仅 http）
//!
//! # 库用法（0.x 实验级 API）
//!
//! 嵌入形态是同步回调 sink：引擎在专用 worker 线程上串行调用回调，
//! 要扇出请自行 spawn。`run` 注册 SIGINT/SIGTERM 优雅停机；
//! `run_with_shutdown` 把停机标志交给调用方。承诺边界见 design §9。
//!
//! ```no_run
//! # fn main() -> Result<(), fnotify::Error> {
//! let config = fnotify::Config::new()
//!     .paths(["/var/log"])
//!     .recursive(true)
//!     .glob("*.log")
//!     .events("create,modify,close_write")?;
//! fnotify::run(
//!     config,
//!     Box::new(|batch: &[fnotify::Event]| {
//!         for ev in batch {
//!             println!("{} {}", ev.event, ev.path.display());
//!         }
//!     }),
//! )?;
//! # Ok(())
//! # }
//! ```

// 生产代码零 unwrap/expect 是质量约定（NFR-04），交给 lint 强制而不是靠评审；
// 测试模块不受此约束（unwrap 集中在 #[cfg(test)] 内）。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod api;
// 内部模块：#[doc(hidden)] 是约定边界——技术上公开（二进制 crate 复用同一实现），
// 但承诺的库公开面只有 Config / run / run_with_shutdown / Event / Error
// （design §9）。升 1.0 时可自由收紧为私有。
#[doc(hidden)]
pub mod cli;
#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod engine;
#[doc(hidden)]
pub mod hook;
#[doc(hidden)]
pub mod locale;
#[doc(hidden)]
pub mod watch;
#[doc(hidden)]
pub mod wildmatch;

pub use api::{Config, Error, run, run_with_shutdown};
pub use engine::{BatchItem as Event, Sink};
