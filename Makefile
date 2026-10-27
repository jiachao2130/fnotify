# Copyright (c) 2026 Jia Chao
# fnotify is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

# fnotify 构建脚本。注意：D1 反转后事件源是 Linux 专有的 inotify，macOS 不能编译——
# check/msrv/dist 都要在 Linux 上跑；本机（macOS）仅可编辑。
#
# vendor : cargo vendor + 把 offline 配置写进 build/.cargo（不污染仓库根）
# dist   : 组装 dist/fnotify-$(VERSION).tar.gz（src + tests + Cargo.toml + Cargo.lock + vendor + .cargo + rpm + LICENSE）
#          （tests/ 随包提供：验收与冒烟可在包内直接跑全量测试）
# check  : cargo fmt --check && cargo clippy -D warnings && cargo test --locked
# msrv   : 用 1.82 工具链做编译级检查
# ci     : 门禁合集 = check + msrv（push 前在 Linux 机器上过一遍）

VERSION ?= 0.1.0
TARBALL := dist/fnotify-$(VERSION).tar.gz
STAGE   := dist/fnotify-$(VERSION)

.PHONY: vendor dist check msrv ci

# MSRV 解析兜底见仓库根 .cargo/config.toml（resolver fallback）。vendor 产物不进
# 该键：cargo 1.82 不认识，每次构建打一条无害 warning。
vendor:
	cargo vendor --locked vendor
	mkdir -p build/.cargo
	printf '%s\n' \
		'[source.crates-io]' \
		'replace-with = "vendored-sources"' \
		'' \
		'[source.vendored-sources]' \
		'directory = "vendor"' \
		> build/.cargo/config.toml

dist: vendor
	rm -rf $(STAGE) $(TARBALL)
	mkdir -p $(STAGE)
	cp -a src tests Cargo.toml Cargo.lock LICENSE README.md fnotify.spec completions $(STAGE)/
	cp -a vendor $(STAGE)/
	cp -a build/.cargo $(STAGE)/.cargo
# macOS 打包时清掉扩展属性，避免 Linux 端 GNU tar 逐文件告警
	if command -v xattr >/dev/null 2>&1; then xattr -cr $(STAGE); fi
	tar -czf $(TARBALL) -C dist fnotify-$(VERSION)
	@echo "=> $(TARBALL)"

check:
	cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked

# MSRV 检查：`cargo +1.82 check --locked`（Linux；macOS 编不了 Linux-only 代码）。
# Cargo.lock 是 MSRV 的真正保障（inotify 精确 pin），勿删勿重新解析。
msrv:
	cargo +1.82 check --locked

ci: check msrv
	@echo "=> ci gate passed"
