// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! lib API 公开面纯度与端到端行为。
//! 纪律：本文件只允许经 `fnotify::` 前缀使用 crate——不许引用任何内部模块
//! （engine/cli/watch/config/hook/locale/wildmatch），公开面回归由 grep 本文件兜底。

use fnotify::{Config, Error, Event};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn sink_noop() -> fnotify::Sink {
    Box::new(|_: &[Event]| {})
}

#[test]
fn events_reuses_cli_value_domain() {
    // 事件订阅与 CLI -e 同域：未知名字是 Config 错误而非 panic。
    let r = Config::new().events("creat");
    assert!(
        matches!(r, Err(Error::Config(_))),
        "未知名应报 Config 错: {r:?}"
    );
    // 合法列表构造成功（builder 消费自身，成功后拿回 Config）。
    let r = Config::new().events("create,modify,close_write");
    assert!(r.is_ok(), "合法事件列表应构造成功: {r:?}");
}

#[test]
fn missing_path_is_config_error() {
    // 不存在的监听路径：Config 错误（CLI 对应 exit 2），不进入运行期。
    let shutdown = Arc::new(AtomicBool::new(false));
    let r = fnotify::run_with_shutdown(
        Config::new().paths(["/nonexistent/fnotify-lib-api-path"]),
        sink_noop(),
        shutdown,
    );
    match r {
        Err(Error::Config(m)) => assert!(m.contains("/nonexistent/fnotify-lib-api-path")),
        other => panic!("应报 Config 错，得到 {other:?}"),
    }
}

#[test]
fn end_to_end_delivery_through_public_surface() {
    // 端到端：临时目录 + shutdown 标志驱动，验证 Event 字段语义与优雅停机。
    let dir = std::env::temp_dir().join(format!("fnotify-lib-api-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::channel::<Vec<Event>>();
    let engine_dir = dir.clone();
    let flag = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || {
        fnotify::run_with_shutdown(
            Config::new().paths([&engine_dir]).debounce_ms(10),
            Box::new(move |batch: &[Event]| {
                let _ = tx.send(batch.to_vec());
            }),
            flag,
        )
    });
    // 等引擎就位后制造一个事件。轮询代替固定 sleep：每轮写一个新文件
    // probe-<i>.txt 并等 200ms（总预算 5s），首个含 create 的批次即算就绪。
    // 每轮用新文件名是因为 watcher 注册前的事件会丢——早期轮次的 create 可能
    // 落空，但必有某轮的 create 被捕获（引擎就绪后）。
    let batch = (0..25)
        .find_map(|i| {
            std::fs::write(dir.join(format!("probe-{i}.txt")), "x").unwrap();
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(b) if b.iter().any(|ev| ev.event == "create") => Some(b),
                _ => None,
            }
        })
        .expect("引擎应交付含 create 的批次");
    assert!(
        batch.iter().any(|ev| {
            ev.event == "create"
                && ev
                    .path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("probe-"))
                && !ev.dir
        }),
        "批次应含 probe-*.txt 的 create: {batch:?}"
    );
    // 优雅停机：置位 → 引擎收尾退出 Ok。
    shutdown.store(true, Ordering::Release);
    let r = handle.join().unwrap();
    assert!(r.is_ok(), "优雅停机应 Ok: {r:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn config_rejects_until_idle_zero_like_cli() {
    // Config::until_idle(0) 与 CLI --until-idle 0 / config until_idle = 0 同表同拒：
    // 0 的语义是"首个窗口即退"，不是"不启用"。
    let shutdown = Arc::new(AtomicBool::new(false));
    let r = fnotify::run_with_shutdown(Config::new().until_idle(0), sink_noop(), shutdown);
    match r {
        Err(Error::Config(m)) => assert!(m.contains("until-idle"), "报错应指名 until-idle: {m}"),
        other => panic!("until_idle(0) 应报 Config 错，得到 {other:?}"),
    }
}

#[test]
fn config_rejects_recursive_nesting_like_cli() {
    // 库构造器可给出 CLI 会 exit 2 的递归嵌套根（外层递归把内层注册两遍，
    // 双 wd 双报）——库侧必须与 CLI 同拒（design §9.1 同表同拒）。
    let base = std::env::temp_dir().join(format!("fnotify-lib-nest-{}", std::process::id()));
    let inner = base.join("inner");
    std::fs::create_dir_all(&inner).unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let r = fnotify::run_with_shutdown(
        Config::new().paths([&base, &inner]).recursive(true),
        sink_noop(),
        shutdown,
    );
    match r {
        Err(Error::Config(m)) => assert!(
            m.contains("nest") || m.contains("嵌套"),
            "报错应指名嵌套: {m}"
        ),
        other => panic!("递归嵌套根应报 Config 错，得到 {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn ready_file_created_via_config_builder() {
    // Config::ready_file 经 builder 生效：全部根（含递归子目录）挂表完成后
    // 就绪文件出现；shutdown 优雅停机后仍在（不删除，与 CLI --ready-file 同语义）。
    let dir = std::env::temp_dir().join(format!("fnotify-lib-ready-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    let ready = std::env::temp_dir().join(format!("fnotify-lib-ready-{}-file", std::process::id()));
    let _ = std::fs::remove_file(&ready);
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let watch_dir = dir.clone();
    let ready_in = ready.clone();
    let handle = std::thread::spawn(move || {
        fnotify::run_with_shutdown(
            Config::new()
                .paths([&watch_dir])
                .recursive(true)
                .ready_file(&ready_in),
            sink_noop(),
            flag,
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        assert!(
            Instant::now() < deadline,
            "5s 内就绪文件未出现（builder 未生效？）"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    shutdown.store(true, Ordering::Release);
    let r = handle.join().unwrap();
    assert!(r.is_ok(), "优雅停机应 Ok: {r:?}");
    assert!(ready.exists(), "停机后就绪文件应仍在（不删除）");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_file(&ready);
}
