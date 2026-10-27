// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! inotify 监听封装：递归注册、新建子目录的动态接管、wd 与路径映射的维护。
//!
//! 事件流不经过线程：inotify fd 非阻塞，注册进 mio Poll 由引擎事件循环单线程
//! 轮询，读到 WouldBlock 即读尽。事件与状态维护（watch 摘除、目录接管、溢出重建）
//! 在同一个读取批次内按序处理，不需要锁。

use crate::engine::{BIT_ACCESS, BIT_CLOSE_WRITE, BIT_OPEN, EVENT_OVERFLOW};
use crate::locale::pick;
use crate::pickf;
use inotify::{EventMask, EventOwned, Inotify, WatchDescriptor, WatchMask};
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const TOKEN: Token = Token(0);
const READ_BUF: usize = 64 * 1024;

/// 一条待投递的原始事件。
pub struct RawEvent {
    /// 事件名：create / modify / remove / access / open / metadata / close_write / rename_from / rename_to；
    /// 另有一个不入事件名字空间的元记录名 [`EVENT_OVERFLOW`]（内核队列溢出，
    /// path 恒为空、is_dir 恒为 false，引擎绕过聚合三道闸直发）。
    pub name: &'static str,
    /// 绝对路径（监听目录启动时已规范化；子路径由内核 wd 加 name 拼出）。
    pub path: PathBuf,
    /// 是否目录，取自内核 IN_ISDIR 标志。
    pub is_dir: bool,
}

/// inotify 监听句柄：fd、Poll、wd↔路径双向映射、待投递队列。
pub struct Watcher {
    ino: Inotify,
    poll: Poll,
    events: Events,
    buf: Vec<u8>,
    mask: WatchMask,
    recursive: bool,
    /// wd → watch 对应的目录路径（事件按此拼绝对路径）。
    wd_map: HashMap<WatchDescriptor, PathBuf>,
    /// 路径 → wd，反向索引：目录移出/删除时按路径撤掉对应 watch。
    by_path: HashMap<PathBuf, WatchDescriptor>,
    /// 已注册的监听目录（队列溢出后按此重建全部 watch）。
    roots: Vec<PathBuf>,
    /// 已删除的监听目录（FIFO，引擎取走后告警并摘除；摘光即退出）。
    removed_roots: VecDeque<PathBuf>,
    /// 本批已读到、待引擎逐条取走的事件。
    pending: VecDeque<RawEvent>,
    /// MOVED_FROM|ISDIR 的未配对暂存（cookie → 旧路径）：配对的 MOVED_TO 到达
    /// 即树内 rename（改写前缀映射）；批量读尽时仍无配对即移出监听树（整棵摘除）。
    pending_moves: HashMap<u32, PathBuf>,
    /// 监听目录自身的 MOVE_SELF 暂存（wd, 路径快照）：批末裁决——本批被配对改写
    /// （renamed_wds）= 树内 rename，不动；否则 = 移出监听树，按删除同语义摘除。
    /// 注意：MOVE_SELF 在内核队列里位于 FROM/TO 之后，快照可能已是改写后的新路径，
    /// 不能靠比较快照与当前映射区分两种情形，必须以 renamed_wds 为准。
    moved_self: Vec<(WatchDescriptor, PathBuf)>,
    /// 本批已被 rename 配对改写的 wd（flush 时与 moved_self 对账后清空）。
    renamed_wds: HashSet<WatchDescriptor>,
}

impl Watcher {
    /// 创建并初始化：inotify fd 置为非阻塞（IN_NONBLOCK），注册进 Poll 等可读。
    pub fn new(recursive: bool, event_mask: u32) -> io::Result<Self> {
        let ino = Inotify::init()?;
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut SourceFd(&ino.as_raw_fd()), TOKEN, Interest::READABLE)?;
        let mut mask = WatchMask::CREATE
            | WatchMask::DELETE
            | WatchMask::MODIFY
            | WatchMask::ATTRIB
            | WatchMask::MOVED_FROM
            | WatchMask::MOVED_TO
            | WatchMask::MOVE_SELF
            | WatchMask::ONLYDIR;
        if event_mask & BIT_ACCESS != 0 {
            mask |= WatchMask::ACCESS;
        }
        if event_mask & BIT_OPEN != 0 {
            mask |= WatchMask::OPEN;
        }
        if event_mask & BIT_CLOSE_WRITE != 0 {
            mask |= WatchMask::CLOSE_WRITE;
        }
        Ok(Watcher {
            ino,
            poll,
            events: Events::with_capacity(16),
            buf: vec![0; READ_BUF],
            mask,
            recursive,
            wd_map: HashMap::new(),
            by_path: HashMap::new(),
            roots: Vec::new(),
            removed_roots: VecDeque::new(),
            pending: VecDeque::new(),
            pending_moves: HashMap::new(),
            moved_self: Vec::new(),
            renamed_wds: HashSet::new(),
        })
    }

    /// 注册一个监听目录：先自身后子目录（自顶向下）。自身注册失败即整体失败；
    /// 子目录失败仅告警，其余目录（含失败目录的可读后代）照常注册。
    pub fn add_root(&mut self, root: &Path) -> io::Result<()> {
        if self.recursive {
            self.watch_tree(root)?; // 起始目录失败上抛（watch_tree 契约）
        } else {
            self.add_dir(root)?;
        }
        self.roots.push(root.to_path_buf());
        Ok(())
    }

    /// 取出一个已删除的监听目录（FIFO）；无则返回 None。
    pub fn next_removed_root(&mut self) -> Option<PathBuf> {
        self.removed_roots.pop_front()
    }

    /// 监听目录是否已全部摘除（引擎据此决定是否退出）。
    pub fn no_roots(&self) -> bool {
        self.roots.is_empty()
    }

    /// 等一条事件，最长 timeout，超时返回 Ok(None)。
    /// timeout 为零时做一轮非阻塞探测（读到即返回，读不到立即返回 None）。
    pub fn poll_event(&mut self, timeout: Duration) -> io::Result<Option<RawEvent>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(ev) = self.pending.pop_front() {
                return Ok(Some(ev));
            }
            let remain = deadline.saturating_duration_since(Instant::now());
            self.events.clear();
            self.poll.poll(&mut self.events, Some(remain))?;
            let readable = self
                .events
                .iter()
                .any(|ev| ev.token() == TOKEN && ev.is_readable());
            if readable {
                self.read_available()?;
            }
            // 队列里只有维护动作（watch 摘除、目录接管等）时不算一条事件；
            // 非阻塞探测读到即返回——但 pending 里刚入队的事件必须逐条交付完
            // 才算读尽，否则停机清空的尾批会在此丢失（回归钉：见末尾单测）。
            if remain.is_zero() && self.pending.is_empty() {
                return Ok(None);
            }
        }
    }

    /// 注册单个目录（完整掩码）。同一（路径, 掩码）重复注册内核返回同一 wd，幂等。
    fn add_dir(&mut self, path: &Path) -> io::Result<()> {
        self.add_dir_masked(path, self.mask)
    }

    /// 按指定掩码注册单个目录，并维护 wd↔路径双向映射。
    fn add_dir_masked(&mut self, path: &Path, mask: WatchMask) -> io::Result<()> {
        let wd = self
            .ino
            .watches()
            .add(path, mask)
            .map_err(|e| enospc_hint(path, e))?;
        self.by_path.insert(path.to_path_buf(), wd.clone());
        self.wd_map.insert(wd, path.to_path_buf());
        Ok(())
    }

    /// 撤掉单个 watch 及其映射。目录被删除时内核自动摘除并补发 IN_IGNORED，
    /// 这里处理的是目录仍存活、只是移出监听树的场景；重复清理由 IGNORED 分支兜底。
    fn remove_dir(&mut self, path: &Path) {
        if let Some(wd) = self.by_path.remove(path) {
            self.wd_map.remove(&wd);
            let _ = self.ino.watches().remove(wd);
        }
    }

    /// 整棵子树摘除：内核 watch 随 inode 存活（移出监听树不触发 IN_IGNORED），
    /// 必须把 root 前缀下的后代 watch 一并撤掉——否则树外活动会以旧路径误报。
    /// 路径前缀比较是组件级的（`Path::starts_with`）。
    fn remove_subtree(&mut self, root: &Path) {
        let doomed: Vec<PathBuf> = self
            .by_path
            .keys()
            .filter(|p| p.starts_with(root))
            .cloned()
            .collect();
        for p in doomed {
            self.remove_dir(&p);
        }
    }

    /// 树内 rename：把 old 前缀下的全部 wd↔路径映射改写到 new 前缀。
    /// 内核 watch 随 inode 存活、wd 不变，仅改路径侧；顺带清除旧路径的
    /// 反向映射残迹——否则旧路径重建后，其下新建子目录会被残迹挡住接管。
    /// roots 里同前缀的表项一并改写，否则改写后的目录被删时认不出是监听目录。
    fn rewrite_prefix(&mut self, old: &Path, new: &Path) {
        let hits: Vec<(PathBuf, WatchDescriptor)> = self
            .by_path
            .iter()
            .filter(|(p, _)| p.starts_with(old))
            .map(|(p, wd)| (p.clone(), wd.clone()))
            .collect();
        for (old_p, wd) in hits {
            let rel = match old_p.strip_prefix(old) {
                Ok(r) => r,
                // starts_with 已判定前缀，理论不可达；防御性跳过而非 panic（NFR-04）。
                Err(_) => continue,
            };
            let new_p = new.join(rel);
            self.by_path.remove(&old_p);
            self.by_path.insert(new_p.clone(), wd.clone());
            self.wd_map.insert(wd.clone(), new_p);
            self.renamed_wds.insert(wd);
        }
        for root in &mut self.roots {
            if let Ok(rel) = root.strip_prefix(old) {
                *root = new.join(rel);
            }
        }
    }

    /// 批末结算：仍无配对 MOVED_TO 的 MOVED_FROM = 目录移出监听树，整棵摘除。
    /// 配对在批量内按序完成（内核 FROM/TO 相邻），读尽即为裁决点。
    /// 监听目录自身的 MOVE_SELF 也在这里裁决：本批被配对改写（renamed_wds）=
    /// 树内 rename，不动；否则 = 移出监听树，按删除同语义摘除（roots 除名 + 待取队列）。
    /// 批末结算未配对的 MOVED_FROM / MOVE_SELF。裁决点是「队列读尽」——内核保证
    /// rename 的 FROM/TO 相邻入队，同一次 read 里必然同时出现，故读尽即无配对
    /// 的判定在自研 watcher 自己读 fd 的前提下成立；理论窗口：配对 TO 因跨 wd
    /// 事件交错延迟到下一次 read（极低概率，未实测复现）时会把树内 rename 误判
    /// 为移出摘除——代价是丢监听而非错报事件，且下一批的 TO 到达时会重新接管，
    /// 按可接受取舍记录在此。
    fn flush_pending_moves(&mut self) {
        let moves: Vec<PathBuf> = self.pending_moves.drain().map(|(_, p)| p).collect();
        for p in moves {
            self.remove_subtree(&p);
        }
        let self_moves = std::mem::take(&mut self.moved_self);
        for (wd, path) in self_moves {
            if self.renamed_wds.contains(&wd) {
                continue; // 本批已配对改写 = 树内 rename，watch 与映射都随新路径走
            }
            // 双向映射仍指向本 wd = watch 随 inode 去了树外（旧路径名下）：整棵摘除；
            // 若旧路径已被同名新目录接管（by_path 指向别的 wd），只除名不摘除。
            if self.by_path.get(&path).is_some_and(|w| w == &wd) {
                self.remove_subtree(&path);
            }
            // wd 已不在映射 = 嵌套根移出：watch 已由未配对 FROM 的批末结算摘除，
            // 这里只除名 roots、记入待取队列。
            if let Some(pos) = self.roots.iter().position(|r| r == &path) {
                self.roots.remove(pos);
                self.removed_roots.push_back(path);
            }
        }
        self.renamed_wds.clear();
    }

    /// 把 fd 上的事件全部读出并入队；fd 非阻塞，读到 WouldBlock 即读尽。
    /// 事件先拷成 owned 再分发，避免借用冲突。
    /// 读尽时结算未配对的 MOVED_FROM（内核保证 rename 的 FROM/TO 相邻入队，
    /// 读尽仍无配对即移出监听树）。
    fn read_available(&mut self) -> io::Result<()> {
        loop {
            match self.ino.read_events(&mut self.buf) {
                Ok(events) => {
                    let owned: Vec<EventOwned> = events.map(|e| e.to_owned()).collect();
                    for ev in &owned {
                        self.dispatch(ev);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.flush_pending_moves();
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 单条内核事件：先维护状态（IGNORED 清理、目录接管、溢出重建），再入待投递队列。
    fn dispatch(&mut self, ev: &EventOwned) {
        let mask = ev.mask;
        if mask.contains(EventMask::Q_OVERFLOW) {
            // 队列已满、事件已丢：重建全部 watch；丢失期间发生的变化不做补偿。
            eprintln!(
                "fnotify: {}",
                pick(
                    "event queue overflow, re-watching all directories",
                    "事件队列溢出，重新注册全部监听目录"
                )
            );
            self.rescan();
            // 元记录入待投递队列（引擎绕过聚合三道闸直发 type:"overflow"
            // 降级行）：每次溢出各一条，把内核侧静默丢事件变为消费方可显式
            // 感知的降级。塞在 rescan 之后，与它在内核事件流中的位置一致。
            self.pending.push_back(RawEvent {
                name: EVENT_OVERFLOW,
                path: PathBuf::new(),
                is_dir: false,
            });
            return;
        }
        if mask.contains(EventMask::IGNORED) {
            // watch 被内核摘除（目录删除/移动/手动移除）：清映射。
            // 命中监听目录的，从 roots 摘除并记录，交由引擎告警；
            // 逐目录摘除后其余目录继续监听，摘光才退出。
            if let Some(path) = self.wd_map.remove(&ev.wd) {
                // 迟到的 IGNORED（旧 wd）防盲删：同路径重建后 by_path 已指向
                // 别的 wd 时，本事件只属于已死的旧 watch，不得摘掉新表项。
                if self.by_path.get(&path).is_none_or(|w| w != &ev.wd) {
                    return;
                }
                self.by_path.remove(&path);
                if let Some(pos) = self.roots.iter().position(|r| r == &path) {
                    self.roots.remove(pos);
                    self.removed_roots.push_back(path);
                }
            }
            return;
        }
        if mask.contains(EventMask::MOVE_SELF) {
            // 监听目录自身被移动。此刻无法区分移出监听树与树内 rename，且内核队列里
            // MOVE_SELF 位于 FROM/TO 之后（rename 的配对改写可能已发生）——
            // 只暂存 wd 与路径快照，批末与 renamed_wds 对账裁决（flush_pending_moves）。
            if let Some(path) = self.wd_map.get(&ev.wd).cloned() {
                if self.roots.iter().any(|r| r == &path)
                    && !self.moved_self.iter().any(|(w, _)| w == &ev.wd)
                {
                    self.moved_self.push((ev.wd.clone(), path));
                }
            }
            return; // 维护事件，不投递
        }
        let Some(parent) = self.wd_map.get(&ev.wd).cloned() else {
            return; // watch 已撤（移出清理先于本事件到达）等场景，丢弃
        };
        let path = match ev.name.as_deref() {
            Some(name) => parent.join(name),
            None => parent.clone(), // 事件对象是 watch 目录自身（如 ATTRIB）
        };
        let is_dir = mask.contains(EventMask::ISDIR);
        if is_dir {
            if mask.contains(EventMask::MOVED_FROM) {
                // 目录被移走：先按 cookie 暂存，等配对的 MOVED_TO 决定是树内
                // rename（改写前缀映射）还是移出监听树（批末读尽时整棵摘除）。
                // 内核 watch 随 inode 存活，此刻摘除与否取决于配对结果。
                // cookie 为 0 理论上不出现（rename 必有 cookie），按移出兜底。
                if ev.cookie != 0 {
                    self.pending_moves.insert(ev.cookie, path.clone());
                } else {
                    self.remove_subtree(&path);
                }
            } else if mask.contains(EventMask::MOVED_TO) {
                // 配对成功 = 树内 rename：内核 watch 随 inode 存活、wd 不变，
                // 只需把旧前缀的映射改写到新前缀，事件随即落新路径。
                if ev.cookie != 0 {
                    if let Some(from) = self.pending_moves.remove(&ev.cookie) {
                        self.rewrite_prefix(&from, &path);
                    }
                }
                // 移入（或 rename 目标）的目录：递归模式下接管其子树。
                if self.recursive && !self.by_path.contains_key(&path) {
                    self.takeover(&path);
                }
            } else if mask.contains(EventMask::CREATE) {
                // 新建的目录：递归模式下接管其子树。
                if self.recursive && !self.by_path.contains_key(&path) {
                    self.takeover(&path);
                }
            }
        }
        let name = if mask.contains(EventMask::CREATE) {
            "create"
        } else if mask.contains(EventMask::DELETE) {
            "remove"
        } else if mask.contains(EventMask::MODIFY) {
            "modify"
        } else if mask.contains(EventMask::ATTRIB) {
            "metadata"
        } else if mask.contains(EventMask::ACCESS) {
            "access"
        } else if mask.contains(EventMask::OPEN) {
            "open"
        } else if mask.contains(EventMask::CLOSE_WRITE) {
            "close_write"
        } else if mask.contains(EventMask::MOVED_FROM) {
            "rename_from"
        } else if mask.contains(EventMask::MOVED_TO) {
            "rename_to"
        } else {
            // CLOSE / CLOSE_NOWRITE 等仍未订阅的内核事件不投递
            // （open 已订阅投递，见上方 OPEN 臂）。
            return;
        };
        self.pending.push_back(RawEvent { name, path, is_dir });
    }

    /// 接管一个新建/移入的目录：注册它及其全部现存子目录。
    /// 起始目录注册失败按现有文案告警后放弃（引擎继续监听其余目录）。
    fn takeover(&mut self, path: &Path) {
        if let Err(e) = self.watch_tree(path) {
            eprintln!(
                "fnotify: {}",
                pickf!(
                    "failed to watch {path} (skipped): {e}",
                    "监听 {path} 失败（跳过该目录）: {e}",
                    path = path.display(),
                    e = e
                )
            );
        }
    }

    /// 注册/接管一棵现存目录树：递归 walker。顺序不变式：对任意目录 D，
    /// 先 add_watch(D) 再 readdir(D) 枚举其子目录——若枚举期间 D 下新建了
    /// 目录，其 CREATE 事件必然落在 D 的 watch 上，由后续读取批次兜住接管；
    /// 反过来"先枚举、后挂表"则存在失聪窗口：新建目录既不在枚举结果里，
    /// CREATE 又因 D 的 watch 未挂而丢失，整棵子树从此永久无事件（回归 s28）。
    /// 返回起始目录自身的注册结果：add_root 契约要求起始失败上抛（引擎报错
    /// 退出）；takeover 路径由调用方按现有文案告警。
    /// 非递归（rescan 的重建路径经 takeover 到此）：只挂目录本身、不下钻
    /// （与 takeover 的 `if self.recursive` 守卫对齐）——无扫描即无自触发，
    /// 直接以完整掩码注册。
    fn watch_tree(&mut self, start: &Path) -> io::Result<()> {
        if !self.recursive {
            return self.add_dir(start);
        }
        let scan = self.scan_mask();
        self.add_dir_masked(start, scan)?;
        self.watch_children(start, scan);
        Ok(())
    }

    /// 扫描期掩码：完整掩码摘除 ACCESS、OPEN 与 ATTRIB。readdir 对目录的
    /// opendir/getdents 与读文件同机制自触发事件——下钻读子目录的 IN_ACCESS
    /// 与 IN_OPEN 以子目录名落在**父目录**的 watch 上、扫描自身目录的落在
    /// 其自己的 watch 上；部分挂载（strictatime、首读更新 atime）下读目录还
    /// 会自触发 IN_ATTRIB。万级目录一次下钻即可产生 2×N 自触发事件灌爆内核
    /// 队列（默认配额 16384，2×N 在 N>8192 时必超），形成 overflow → rescan →
    /// 再扫描 → 再灌爆的自激循环并
    /// 楔死单线程 dispatch（s32 实证）；OPEN 不摘则启动扫描自带幻影 open
    /// （s33）。掩码里 CREATE/MOVED_* 全保留——不变式只依赖目录类事件。
    fn scan_mask(&self) -> WatchMask {
        self.mask - (WatchMask::ACCESS | WatchMask::OPEN | WatchMask::ATTRIB)
    }

    /// 目录枚举完成后把 watch 升级回完整掩码：对同一路径重复 add 内核返回
    /// 同一 wd 并原地更新掩码，不产生事件。仅当该目录确有 watch 时升级——
    /// add 失败的目录保持无 watch，不借升级复活（维持既有告警语义）。
    /// 升级失败的瞬态（目录已消失等）watch 本就随之失效，由 IGNORED 兜底。
    fn upgrade_dir(&mut self, path: &Path) {
        if self.by_path.contains_key(path) {
            let _ = self.add_dir_masked(path, self.mask);
        }
    }

    /// 枚举 dir 的子目录并逐个"先挂再下钻"；dir 自身已挂扫描期掩码。
    /// 升级是**后序**的：整个子树 DFS 完成后才把 dir 恢复完整掩码——下钻读
    /// 子目录的 opendir/getdents 会在父目录的 watch 上触发 ACCESS/OPEN（带
    /// 子目录名），前序升级会让这些自触发落在 FULL 掩码上：万级树一次下钻
    /// 即灌爆内核队列、overflow→rescan→再灌爆自激并楔死 dispatch（s32）。
    /// 代价：目录自身的 ACCESS/OPEN/ATTRIB 盲窗从"自身枚举期"扩为"整个
    /// 子树扫描期"（取舍记录见 design.md §3.4）。
    /// 子目录 add 失败仅告警并继续下钻——add 失败不等于不可读，后代目录
    /// 照挂；readdir 失败（如 EACCES）短路该子树并告警一行（s27 固化的
    /// 行为），但**先升级再返回**——瞬态失败（fd 耗尽/EIO 等）下目录仍可
    /// watch，停在扫描掩码会让它收不到 ACCESS/OPEN/ATTRIB（升级机制的
    /// 钉子见 tests 单测 scan_mask_watch_upgrades_to_full_mask）。
    /// entry 的 file_type 不跟随符号链接，天然防环。
    fn watch_children(&mut self, dir: &Path, scan: WatchMask) {
        let rd = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) => {
                eprintln!(
                    "fnotify: {}",
                    pickf!(
                        "cannot scan {path} (subtree not watched): {e}",
                        "无法扫描 {path}（其子树不在监听范围）: {e}",
                        path = dir.display(),
                        e = e
                    )
                );
                // 没下钻就没有自触发源，直接升级恢复完整掩码。
                self.upgrade_dir(dir);
                return;
            }
        };
        let subdirs: Vec<PathBuf> = rd
            .flatten()
            .filter_map(|ent| {
                let ft = ent.file_type().ok()?;
                ft.is_dir().then(|| ent.path())
            })
            .collect();
        for p in subdirs {
            if let Err(e) = self.add_dir_masked(&p, scan) {
                eprintln!(
                    "fnotify: {}",
                    pickf!(
                        "failed to watch {path} (skipped): {e}",
                        "监听 {path} 失败（跳过该目录）: {e}",
                        path = p.display(),
                        e = e
                    )
                );
            }
            self.watch_children(&p, scan);
        }
        // 后序升级：子树扫完，此后不再 read 任何后代，自触发源消失。
        self.upgrade_dir(dir);
    }

    /// 队列溢出后的重建：清空全部映射，按磁盘现状重新注册。
    /// 溢出期间已被删除的监听目录走删除退出语义（内核侧旧 watch 不再认领，
    /// 其事件在 dispatch 因查无 wd 被丢弃），不留零 watch 僵尸。
    fn rescan(&mut self) {
        self.wd_map.clear();
        self.by_path.clear();
        self.pending_moves.clear();
        self.moved_self.clear();
        self.renamed_wds.clear();
        let roots = std::mem::take(&mut self.roots);
        for root in roots {
            if root.is_dir() {
                self.takeover(&root);
                self.roots.push(root);
            } else {
                self.removed_roots.push_back(root);
            }
        }
    }
}

/// ENOSPC（Linux errno 28，本项目仅 Linux）落在 watch 注册上不是磁盘满，
/// 是 inotify 监听配额耗尽——递归注册大树必踩。裸传出去只会显示
/// "No space left on device"，与磁盘满混淆；补上人话与修法。
fn enospc_hint(path: &Path, e: io::Error) -> io::Error {
    if e.raw_os_error() != Some(ENOSPC) {
        return e;
    }
    let cur = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "?".into());
    io::Error::new(
        e.kind(),
        pickf!(
            "inotify watch quota exhausted at {path} (kernel limit, not disk space); fs.inotify.max_user_watches is {cur} — raise it, e.g. sysctl -w fs.inotify.max_user_watches=<n>, and persist under /etc/sysctl.d/",
            "inotify 监听配额耗尽（{path}，内核限制而非磁盘满）；当前 fs.inotify.max_user_watches={cur}——调大后重试，如 sysctl -w fs.inotify.max_user_watches=<数值>，并写入 /etc/sysctl.d/ 持久化",
            path = path.display(),
            cur = cur
        ),
    )
}

const ENOSPC: i32 = 28; // Linux-only crate，无 libc 依赖

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::DEFAULT_EVENTS;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// 唯一临时目录（pid + 序号 + tag），Drop 时递归清理。
    struct Tmp(PathBuf);

    impl Tmp {
        fn new(tag: &str) -> Tmp {
            let d = std::env::temp_dir().join(format!(
                "fnotify-watch-{}-{}-{tag}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&d).expect("创建临时目录");
            Tmp(d)
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// 回归（停机清空曾丢尾批）：零超时探测把一批事件读入 pending 后必须
    /// 逐条交付完才允许返回 None。修复前读到即返回 None，尾批滞留 pending
    /// 被丢弃——本测试在旧实现下 got == 0。
    #[test]
    fn zero_timeout_probe_delivers_full_pending_batch() {
        let dir = Tmp::new("zero");
        let mut w = Watcher::new(false, DEFAULT_EVENTS).expect("创建 watcher");
        w.add_root(&dir.0).expect("注册监听目录");
        for i in 0..3 {
            fs::write(dir.0.join(format!("f{i}.log")), b"x").expect("写入");
        }
        // inotify 事件同步写入 fd 缓冲，此处无需长等；200ms 仅是保险。
        std::thread::sleep(Duration::from_millis(200));
        let mut creates = 0;
        while let Ok(Some(ev)) = w.poll_event(Duration::ZERO) {
            if ev.name == "create" {
                creates += 1;
            }
        }
        assert_eq!(creates, 3, "3 次创建应全数交付（含尾批），实际 {creates}");
    }

    /// 在 deadline 内等到一个已删除的监听目录（watcher 侧已同步摘出 roots）。
    fn wait_removed(w: &mut Watcher) -> PathBuf {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(p) = w.next_removed_root() {
                return p;
            }
            let _ = w.poll_event(Duration::from_millis(50));
            assert!(
                Instant::now() < deadline,
                "2s 内未等到 IN_IGNORED（监听目录删除）"
            );
        }
    }

    fn file_name_of(p: &Path) -> String {
        p.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    #[test]
    fn removed_root_drops_from_root_list() {
        // 逐目录摘除：删掉一个根，roots 里去掉它、no_roots 为 false；
        // 删光后 no_roots 为 true，引擎据此退出。
        let a = Tmp::new("root-a");
        let b = Tmp::new("root-b");
        let mut w = Watcher::new(false, DEFAULT_EVENTS).expect("创建 watcher");
        w.add_root(&a.0).expect("注册根 a");
        w.add_root(&b.0).expect("注册根 b");

        fs::remove_dir_all(&a.0).expect("删除根 a");
        let removed = wait_removed(&mut w);
        assert_eq!(removed, a.0, "待取队列应给出被删的根 a");
        assert!(!w.no_roots(), "根 b 仍在监听，不应判空");

        fs::remove_dir_all(&b.0).expect("删除根 b");
        let removed = wait_removed(&mut w);
        assert_eq!(removed, b.0, "待取队列应给出被删的根 b");
        assert!(w.no_roots(), "两个根都删了，应判空（引擎退出条件）");
    }

    #[test]
    fn tree_rename_of_nested_root_rewrites_root_entry() {
        // 嵌套根（父目录也是监听目录）被树内 rename：roots 表项必须随映射一并改写，
        // 否则改写后的目录被删时认不出是监听目录，摘除失去依据。
        let base = Tmp::new("nest");
        let sub = base.0.join("sub");
        fs::create_dir(&sub).expect("创建子目录");
        let mut w = Watcher::new(false, DEFAULT_EVENTS).expect("创建 watcher");
        w.add_root(&base.0).expect("注册父根");
        w.add_root(&sub).expect("注册嵌套根");

        fs::rename(&sub, base.0.join("renamed")).expect("树内 rename");
        // 消费掉本批事件（含配对改写），等 rename_to 出现即改写已发生。
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match w.poll_event(Duration::from_millis(50)) {
                Ok(Some(ev)) if ev.name == "rename_to" => break,
                _ => assert!(Instant::now() < deadline, "2s 内未等到 rename_to"),
            }
        }
        assert!(
            !w.roots.iter().any(|r| r.ends_with("sub")),
            "旧路径不得残留在 roots"
        );
        assert!(
            w.roots.iter().any(|r| r.ends_with("renamed")),
            "roots 应已改写为新路径"
        );
    }

    #[test]
    fn moved_out_root_is_dropped_like_deleted() {
        // 监听目录被 mv 出监听树（IN_MOVE_SELF 探测）：按删除同语义摘除——
        // roots 除名、记入待取队列；旧路径上重建的同名目录不得被接管。
        let a = Tmp::new("mvout-a");
        let b = Tmp::new("mvout-b");
        let outside =
            std::env::temp_dir().join(format!("fnotify-watch-mvout-{}", std::process::id()));
        let _ = fs::remove_dir_all(&outside);
        let mut w = Watcher::new(false, DEFAULT_EVENTS).expect("创建 watcher");
        w.add_root(&a.0).expect("注册根 a");
        w.add_root(&b.0).expect("注册根 b");

        fs::rename(&a.0, &outside).expect("mv 出监听树");
        let removed = wait_removed(&mut w);
        assert_eq!(removed, a.0, "待取队列应给出被移走的根 a");
        assert!(!w.no_roots(), "根 b 仍在监听，不应判空");

        // 旧路径重建同名目录：其中活动不得上报（watcher 已整体摘除）。
        fs::create_dir(&a.0).expect("旧路径重建目录");
        fs::write(a.0.join("ghost.log"), b"x").expect("重建目录内写文件");
        std::thread::sleep(Duration::from_millis(200));
        while let Ok(Some(ev)) = w.poll_event(Duration::ZERO) {
            assert_ne!(
                file_name_of(&ev.path),
                "ghost.log",
                "已摘除的根的旧路径上不得有事件上报"
            );
        }
        let _ = fs::remove_dir_all(&outside);
    }

    #[test]
    fn enospc_hint_explains_watch_quota() {
        let e = io::Error::from_raw_os_error(ENOSPC);
        let wrapped = enospc_hint(Path::new("/data/big"), e);
        let msg = wrapped.to_string();
        assert!(msg.contains("/data/big"), "应带路径上下文: {msg}");
        assert!(msg.contains("max_user_watches"), "应给出修法: {msg}");
    }

    /// 回归：迟到的 IGNORED（旧 wd）不得误删同路径重建后的新表项。单 fd 下
    /// 内核事件 FIFO 使真实内核难以造出该序列（删除的 IGNORED 必先于重建的
    /// 事件入队），故直接注入 Event 验证状态机：旧 wd 的 IGNORED 到达时，
    /// by_path 若已把该路径指向别的 wd（重建后被重新接管），只清 wd_map 侧，
    /// 不得盲删新表项——否则重建目录的 watch 从映射失联、永久失聪（与接管
    /// 丢目录同族）。
    #[test]
    fn stale_ignored_does_not_delete_recreated_entry() {
        let a = Tmp::new("stale-ignored");
        let mut w = Watcher::new(false, DEFAULT_EVENTS).expect("创建 watcher");
        w.add_root(&a.0).expect("注册监听目录");
        let stale_wd = w.by_path.get(&a.0).expect("起始表项").clone();

        // 删除后重建同名目录并重新接管：新 inode 得到新 wd（旧 watch 已随
        // inode 销毁自动摘除，其 IGNORED 尚留在 fd 队列里未读出）。
        fs::remove_dir_all(&a.0).expect("删除监听目录");
        fs::create_dir(&a.0).expect("重建同名目录");
        w.takeover(&a.0);
        let fresh_wd = w.by_path.get(&a.0).expect("重建后的新表项").clone();
        assert_ne!(
            stale_wd, fresh_wd,
            "重建应得到新 wd（wd 被内核复用会破坏本测试前提）"
        );

        let ev = EventOwned {
            wd: stale_wd,
            mask: EventMask::IGNORED,
            cookie: 0,
            name: None,
        };
        w.dispatch(&ev);

        assert!(
            w.by_path.contains_key(&a.0),
            "迟到的 IGNORED（旧 wd）不得摘掉重建后的新表项 by_path"
        );
        assert!(
            w.roots.iter().any(|r| r == &a.0),
            "旧 wd 的 IGNORED 不得把仍在监听的同路径根从 roots 除名"
        );
    }

    /// 回归（非递归 + 溢出重建）：rescan 无条件走 takeover，watch_tree 必须
    /// 尊重非递归模式——只重挂根目录本身，不下钻子树。旧 takeover 内的
    /// `if self.recursive { collect_dirs }` 守卫在重写为 walker 时一度丢失：
    /// 非递归模式一次溢出重建即把整棵树递归挂表（大树冲爆 watch 配额，
    /// 且相对 0.1.0 静默改变行为）。
    #[test]
    fn rescan_non_recursive_watches_root_only() {
        let dir = Tmp::new("rescan-norec");
        fs::create_dir_all(dir.0.join("a").join("b")).expect("建两层子目录");
        let mut w = Watcher::new(false, DEFAULT_EVENTS).expect("创建 watcher");
        w.add_root(&dir.0).expect("注册监听目录");
        assert_eq!(w.by_path.len(), 1, "非递归启动只挂根目录");

        w.rescan();

        assert!(
            w.by_path.contains_key(&dir.0),
            "溢出重建后根目录自身必须在表"
        );
        assert_eq!(
            w.by_path.len(),
            1,
            "非递归模式的溢出重建不得下钻子树，实际挂了: {:?}",
            w.by_path
                .keys()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
        );
    }

    /// 回归（扫描期掩码升级机制）：upgrade_dir 必须把扫描期 watch 恢复成
    /// 完整掩码——扫描期摘除的 ACCESS 位在升级后必须重新生效。这是
    /// watch_children 后序升级与 readdir 失败分支共同依赖的机制。
    ///
    /// 另注（为何 readdir 失败分支没有直接红测）：不存在"add_watch 成功而
    /// readdir 因权限失败"的静态构造——内核对目录 watch 与 readdir 要求
    /// 同一个 read 位：chmod 000/111 下 add_watch 直接 EACCES（报
    /// failed to watch 而非 cannot scan，0.1.0 同样如此，"权限恢复后事件
    /// 在场"在任何版本都不可构造）；且升级调用在目录不可读时同样 EACCES，
    /// 权限型失败下该分支的升级是空操作。分支实际由瞬态失败触达（fd 耗尽/
    /// EIO/ENOMEM），无法在并行测试进程里安全注入（全局 setrlimit 会波及
    /// 并发用例），故分支里的升级作为纵深防御保留。
    #[test]
    fn scan_mask_watch_upgrades_to_full_mask() {
        let base = Tmp::new("upg");
        let sub = base.0.join("sub");
        fs::create_dir(&sub).expect("创建子目录");
        fs::write(sub.join("f.log"), b"x").expect("写入已知文件");

        let mut w = Watcher::new(true, crate::engine::BIT_ACCESS).expect("创建 watcher");
        let scan = w.scan_mask();
        w.add_dir_masked(&sub, scan).expect("挂扫描期 watch");
        w.upgrade_dir(&sub); // 被测行为：恢复完整掩码

        fs::read(sub.join("f.log")).expect("读已知文件");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Ok(Some(ev)) = w.poll_event(Duration::from_millis(50)) {
                if ev.name == "access" && ev.path.ends_with("f.log") {
                    break;
                }
                continue;
            }
            assert!(
                Instant::now() < deadline,
                "2s 内未等到 f.log 的 access 事件（升级未恢复完整掩码）"
            );
        }
    }

    #[test]
    fn enospc_hint_passes_other_errors_through() {
        // 非配额错误原样透传，不附加提示、保留原始 errno。
        let e = io::Error::from_raw_os_error(2); // ENOENT
        let wrapped = enospc_hint(Path::new("/x"), e);
        assert_eq!(wrapped.raw_os_error(), Some(2));
    }
}
