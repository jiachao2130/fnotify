# Copyright (c) 2026 Jia Chao
# fnotify is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

%define debug_package %{nil}

Name:           fnotify
Version:        0.1.0
# 发行版无关，NVR 不挂 dist 后缀
Release:        1
Summary:        File event router: watch -> debounce -> glob filter -> JSONL / exec

License:        MulanPSL-2.0
URL:            https://github.com/jiachao2130/fnotify
Source0:        %{name}-%{version}.tar.gz
# 申威（sw64）支持：给 vendored libc 0.2.190 补 sw64 信号号 / socket 类型定义。
# 补丁内部按 target_arch = "sw64" 门控，其它架构应用后产物不变；
# 对应补丁目标的 new/glibc sysdeps 布局仅 0.2.190 起存在，升 libc 须重制补丁
Patch0:         libc-0.2.190-sw64.patch
BuildRequires:  rust
BuildRequires:  cargo
BuildRequires:  patch

%description
fnotify watches directories via inotify (Linux), debounces and
aggregates events, filters them by filename glob, then emits JSONL on stdout
and/or runs a command per event. Synchronous thread model,
five direct dependencies (inotify, mio, signal-hook, toml, libc).

%prep
%autosetup -p1

%build
cargo build --release

%install
install -D -m 0755 target/release/fnotify %{buildroot}%{_bindir}/fnotify
install -D -m 0644 completions/fnotify.bash %{buildroot}%{_datadir}/bash-completion/completions/fnotify
install -D -m 0644 completions/_fnotify %{buildroot}%{_datadir}/zsh/site-functions/_fnotify

%files
%license LICENSE
%doc README.md
%{_bindir}/fnotify
%{_datadir}/bash-completion/completions/fnotify
%{_datadir}/zsh/site-functions/_fnotify

%changelog
* Fri Oct 16 2026 Jia Chao <jiachao2130@126.com> - 0.1.0-1
- Initial package: inotify recursive watch, debounce aggregation, glob
  filter, JSONL/exec/webhook actions, oneshot/until-idle, TOML config,
  bilingual messages, hardening (CRLF rejection, bounded shutdown drain
  and channel, EACCES subtree warning, byte-exact %p paths,
  process-group kill), -- terminator.
