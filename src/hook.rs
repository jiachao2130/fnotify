// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! webhook：把每个去抖批次以 JSON 数组经 HTTP POST 推送到远端。
//!
//! 只支持 `http://`——手写最小 HTTP/1.1 客户端（`std::net::TcpStream`），
//! 不引入 TLS 依赖；`https://` 需要 TLS 栈，届时再评审（design §6）。
//! 连接、读写、等响应各有硬上限 10s（等响应为整体墙钟，防慢滴流拖长），
//! 失败由调用方记 stderr 后继续，不打断事件流。

use crate::engine::{jsonl_line, BatchItem};
use crate::locale::pick;
use crate::pickf;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

/// 连接与读写的整体超时上限：webhook 在动作 worker 上同步执行，不能让
/// 一个死掉的远端把事件流拖死。
const TIMEOUT: Duration = Duration::from_secs(10);

/// 解析后的 http 端点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    /// 请求目标（path + query），至少为 "/"。
    pub target: String,
}

/// 报错回显用：控制字符按转义形态显示（`\r`、`\n`、`\t`），不把注入字节
/// 直接打到终端——URL 已被拒绝，回显只需可辨识，不必逐字节复述原文。
fn show_url(url: &str) -> String {
    url.chars().flat_map(char::escape_debug).collect()
}

/// 解析 `--webhook URL` 取值。仅 `http://`；`https://` 明确拒绝并说明原因。
pub fn parse_endpoint(url: &str) -> Result<Endpoint, String> {
    let rest = url.strip_prefix("http://").ok_or_else(|| {
        if url.starts_with("https://") {
            pick(
                "https webhook is not supported (would need a TLS dependency); use http://",
                "不支持 https webhook（需引入 TLS 依赖），请使用 http://",
            )
            .into()
        } else {
            pickf!(
                "invalid webhook URL: {url} (must start with http://)",
                "非法 webhook URL: {url}（须以 http:// 开头）",
                url = show_url(url)
            )
        }
    })?;
    // host[:port] 与 target 分界：authority 到第一个 '/' 为止。
    let (authority, target) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return Err(pickf!(
            "invalid webhook URL: {url} (empty host)",
            "非法 webhook URL: {url}（主机为空）",
            url = show_url(url)
        ));
    }
    let (host, port) = match authority.rsplit_once(':') {
        // 出现冒号即必须是数字端口（IPv6 字面量不在支持范围）。
        Some((h, p)) => {
            let port: u16 = p.parse().map_err(|_| {
                pickf!(
                    "invalid webhook URL: {url} (bad port)",
                    "非法 webhook URL: {url}（端口非法）",
                    url = show_url(url)
                )
            })?;
            (h, port)
        }
        None => (authority, 80),
    };
    if host.is_empty() {
        return Err(pickf!(
            "invalid webhook URL: {url} (empty host)",
            "非法 webhook URL: {url}（主机为空）",
            url = show_url(url)
        ));
    }
    // 主机字符白名单之外的（控制字符、空白、CR/LF 注入）与裸冒号（IPv6 字面量
    // 不支持，多个冒号也落在这里）一律拒绝——host 会原样进 Host 头与连接目标。
    if host.chars().any(|c| c <= ' ' || c == '\x7f' || c == ':') {
        return Err(pickf!(
            "invalid webhook URL: {url} (bad host)",
            "非法 webhook URL: {url}（主机名非法）",
            url = show_url(url)
        ));
    }
    // target 同样要挡：它会原样拼进请求行 `POST {target} HTTP/1.1`，一个
    // CR/LF 就能注入伪造头（host 侧已挡，这里补齐不对称缺口）
    if target.chars().any(|c| c <= ' ' || c == '\x7f') {
        return Err(pickf!(
            "invalid webhook URL: {url} (bad path)",
            "非法 webhook URL: {url}（路径非法）",
            url = show_url(url)
        ));
    }
    Ok(Endpoint {
        host: host.to_string(),
        port,
        target: target.to_string(),
    })
}

/// POST 一个批次（JSON 数组，元素与 JSONL 行同构）。返回 Err 即推送失败。
pub fn post_batch(ep: &Endpoint, items: &[BatchItem]) -> Result<(), String> {
    let mut body = String::with_capacity(items.len() * 64 + 2);
    body.push('[');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&jsonl_line(item));
    }
    body.push(']');

    // 连接阶段超时：TcpStream::connect 不受 read/write_timeout 约束，黑洞地址会
    // 拖到内核 TCP 默认（约 2 分钟）——逐地址 connect_timeout，上限 TIMEOUT。
    let deadline = Instant::now() + TIMEOUT;
    let mut last_err: Option<std::io::Error> = None;
    let mut stream: Option<TcpStream> = None;
    for addr in (ep.host.as_str(), ep.port)
        .to_socket_addrs()
        .map_err(|e| connect_err(ep, e))?
    {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            break;
        }
        match TcpStream::connect_timeout(&addr, remain.min(TIMEOUT)) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let mut stream = match stream {
        Some(s) => s,
        None => {
            return Err(connect_err(
                ep,
                last_err.unwrap_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out")
                }),
            ))
        }
    };
    let _ = stream.set_write_timeout(Some(TIMEOUT));

    // 非 80 端口的 Host 头必须带端口（HTTP/1.1 Host 语义）。
    let host_header = if ep.port == 80 {
        ep.host.clone()
    } else {
        format!("{}:{}", ep.host, ep.port)
    };
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        ep.target,
        host_header,
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .and_then(|_| stream.write_all(body.as_bytes()))
        .and_then(|_| stream.flush())
        .map_err(|e| io_err(ep, e))?;

    // Connection: close——读到 EOF 即响应完毕；只需要状态行判别成败。
    // 逐次读并卡整体墙钟：单次 read 超时只说明这一段没数据，慢滴流凑满
    // TIMEOUT 即报错，不让一个喂字节的远端无限拖住动作 worker。
    let mut resp = Vec::with_capacity(256);
    let mut buf = [0u8; 4096];
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            return Err(pickf!(
                "webhook {host}:{port} response timed out after {sec}s",
                "webhook {host}:{port} 响应超时（{sec}s）",
                host = ep.host,
                port = ep.port,
                sec = TIMEOUT.as_secs()
            ));
        }
        let _ = stream.set_read_timeout(Some(remain.min(TIMEOUT)));
        match stream.read(&mut buf) {
            Ok(0) => break, // EOF：响应完毕
            Ok(n) => {
                resp.extend_from_slice(&buf[..n]);
                if resp.len() > 64 * 1024 {
                    break; // 头部早该出现，防恶意/异常大响应占内存
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue; // 本轮读超时：回到循环头重查墙钟
            }
            Err(e) => return Err(io_err(ep, e)),
        }
    }
    let status = String::from_utf8_lossy(&resp);
    let code: u16 = status
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            pickf!(
                "webhook {host} returned an unreadable response",
                "webhook {host} 返回了无法解析的响应",
                host = ep.host
            )
        })?;
    if !(200..300).contains(&code) {
        return Err(pickf!(
            "webhook {host} returned HTTP {code}",
            "webhook {host} 返回 HTTP {code}",
            host = ep.host,
            code = code
        ));
    }
    Ok(())
}

fn connect_err(ep: &Endpoint, e: std::io::Error) -> String {
    pickf!(
        "webhook connect to {host}:{port} failed: {e}",
        "webhook 连接 {host}:{port} 失败: {e}",
        host = ep.host,
        port = ep.port,
        e = e
    )
}

fn io_err(ep: &Endpoint, e: std::io::Error) -> String {
    pickf!(
        "webhook I/O with {host}:{port} failed: {e}",
        "webhook 与 {host}:{port} 的 I/O 失败: {e}",
        host = ep.host,
        port = ep.port,
        e = e
    )
}

#[cfg(test)]
mod tests {
    use super::{parse_endpoint, post_batch, Endpoint};
    use crate::engine::BatchItem;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::mpsc;

    #[test]
    fn endpoint_parsing() {
        assert_eq!(
            parse_endpoint("http://example.com/hook").unwrap(),
            Endpoint {
                host: "example.com".into(),
                port: 80,
                target: "/hook".into()
            }
        );
        assert_eq!(
            parse_endpoint("http://127.0.0.1:9090/x?y=1").unwrap(),
            Endpoint {
                host: "127.0.0.1".into(),
                port: 9090,
                target: "/x?y=1".into()
            }
        );
        // 无路径时 target 为 "/"
        assert_eq!(parse_endpoint("http://example.com").unwrap().target, "/");
        // 非法：非 http / https / 空 host / 坏端口 / 控制字符与裸冒号主机
        assert!(parse_endpoint("https://example.com/").is_err());
        assert!(parse_endpoint("ftp://example.com/").is_err());
        assert!(parse_endpoint("http://").is_err());
        assert!(parse_endpoint("http://example.com:abc/").is_err());
        assert!(parse_endpoint("http://:8080/").is_err());
        assert!(parse_endpoint("http://exa\r\nmple.com/").is_err());
        assert!(parse_endpoint("http://a:b:c/").is_err());
        // target 侧注入：path/query 带 CR/LF/空格会拼进请求行伪造头（host 侧
        // 已挡，这里补齐不对称缺口）
        assert!(parse_endpoint("http://example.com/hook\r\nX-Injected: 1").is_err());
        assert!(parse_endpoint("http://example.com/\nHost: evil").is_err());
        assert!(parse_endpoint("http://example.com/a b").is_err());
        assert!(parse_endpoint("http://example.com/a\x7f").is_err());
        // 合法 target（含 query 与 % 编码）不受影响
        assert!(parse_endpoint("http://example.com/a%20b?x=1&y=%2f").is_ok());
        // 拒绝消息的回显不得含原始控制字符——按转义形态显示（show_url），
        // 注入字节不应原样打到终端
        let e = parse_endpoint("http://exa\r\nmple.com/").unwrap_err();
        assert!(!e.contains(['\r', '\n']), "回显不得含原始 CR/LF: {e:?}");
        assert!(e.contains("\\r\\n"), "回显应为转义形态: {e:?}");
        let e = parse_endpoint("http://example.com/\nHost: evil").unwrap_err();
        assert!(!e.contains('\n'), "回显不得含原始 LF: {e:?}");
        assert!(e.contains("\\n"), "回显应为转义形态: {e:?}");
    }

    fn item(name: &str) -> BatchItem {
        BatchItem {
            ts: 1,
            event: "create",
            path: PathBuf::from(format!("/tmp/{name}")),
            dir: false,
        }
    }

    /// 起一次性 HTTP 服务器：收到请求后把请求原文送回测试线程，并按
    /// `status` 应答。返回 (端点, 接收到的请求原文)。
    fn one_shot_server(status: &str) -> (Endpoint, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("绑定临时端口");
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        let status = status.to_string();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("接受连接");
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            // 循环读到 headers 完整且 body 达到 Content-Length 声明的长度
            //（TCP 不保证一次 read 拿到整个报文）。
            let _body = loop {
                if let Some(body) = parse_http_request(&got) {
                    break body;
                }
                let n = sock.read(&mut buf).expect("读请求");
                assert!(n > 0, "连接意外关闭");
                got.extend_from_slice(&buf[..n]);
            };
            let _ = tx.send(String::from_utf8_lossy(&got).into_owned());
            let resp = format!("{status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let _ = sock.write_all(resp.as_bytes());
        });
        (
            Endpoint {
                host: "127.0.0.1".into(),
                port,
                target: "/hook".into(),
            },
            rx,
        )
    }

    /// 从原始字节里取请求体；headers 未完整或 body 未收齐时返回 None。
    fn parse_http_request(got: &[u8]) -> Option<String> {
        let text = String::from_utf8_lossy(got);
        let (head, body) = text.split_once("\r\n\r\n")?;
        let len: usize = head
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().to_string())
            })?
            .parse()
            .ok()?;
        (body.len() >= len).then(|| body.to_string())
    }

    #[test]
    fn post_batch_sends_json_array() {
        let (ep, rx) = one_shot_server("HTTP/1.1 200 OK");
        post_batch(&ep, &[item("a.log"), item("b.log")]).expect("POST 应成功");
        let text = rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("应收到请求");
        // 非 80 端口的 Host 头必须带端口（HTTP/1.1 语义）。
        assert!(
            text.contains(&format!("Host: 127.0.0.1:{}\r\n", ep.port)),
            "Host 头应含端口，实际 {text}"
        );
        let body = parse_http_request(text.as_bytes()).expect("请求应完整");
        // 元素与事件 JSONL 行同构：type 判别字段先行，ts 为 RFC3339 UTC。
        assert_eq!(
            body,
            r#"[{"type":"event","ts":"1970-01-01T00:00:00.001Z","event":"create","path":"/tmp/a.log","dir":false},{"type":"event","ts":"1970-01-01T00:00:00.001Z","event":"create","path":"/tmp/b.log","dir":false}]"#
        );
    }

    #[test]
    fn post_batch_non_2xx_is_error() {
        let (ep, _rx) = one_shot_server("HTTP/1.1 500 Internal Server Error");
        let err = post_batch(&ep, &[item("a.log")]).expect_err("5xx 应报错");
        assert!(err.contains("500"), "错误应含状态码，实际 {err}");
    }
}
