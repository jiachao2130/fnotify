#!/usr/bin/env bash
# 一键打 fnotify SRPM：git archive 出源码 → 组装离线 vendor → rpmbuild -bs。
# 用法：bash scripts/make-srpm.sh        （需干净工作树；源码 tar 取自 HEAD；产物路径在最后输出）
# 可选：RPM_TOPDIR=/path 覆盖 rpmbuild 顶层目录（默认 ~/rpmbuild）。
#
# vendor 不在 git 里（git archive 无此目录），在临时 staging 组装进 tar：
#   - pristine `cargo vendor --locked`
#   - .cargo/config.toml 覆写为 vendored 重定向（仓库 tracked 版本只有 MSRV
#     resolver 设置，不带离线语义）
#   - 存在 patches/ 时（msrv-1.82 分支携带的 sw64 补丁）：补丁由 spec 的
#     Patch0 在 %prep 阶段改写 vendored libc，必须清空 libc 的文件级校验表；
#     包级校验仍走 Cargo.lock 的 sha256。补丁同时拷入 SOURCES 供 rpmbuild 取用
set -euo pipefail
cd "$(dirname "$0")/.."

command -v rpmbuild >/dev/null 2>&1 || { echo "错误：缺 rpmbuild（dnf install rpm-build）"; exit 1; }
command -v cargo >/dev/null 2>&1 || { echo "错误：缺 cargo（dnf install cargo）"; exit 1; }
[[ -z $(git status --porcelain) ]] || { echo "错误：工作树不干净（tar 取自 HEAD），先提交"; exit 1; }

V="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
TARBALL="fnotify-$V.tar.gz"
TOP="${RPM_TOPDIR:-$HOME/rpmbuild}"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
git archive --prefix="fnotify-$V/" HEAD | tar -x -C "$STAGE"
S="$STAGE/fnotify-$V"

(cd "$S" && cargo vendor --locked vendor) >/dev/null
printf '%s\n' \
	'[source.crates-io]' \
	'replace-with = "vendored-sources"' \
	'' \
	'[source.vendored-sources]' \
	'directory = "vendor"' \
	> "$S/.cargo/config.toml"

EXTRA_SOURCES=()
if [[ -d patches ]]; then
	python3 - "$S/vendor/libc/.cargo-checksum.json" <<'EOF'
import json, re, sys
lock = open("Cargo.lock").read()
m = re.search(r'name = "libc"\nversion = "[^"]+"\nsource = "registry[^"]*"\nchecksum = "([0-9a-f]{64})"', lock)
assert m, "Cargo.lock: libc checksum 行缺失"
json.dump({"files": {}, "package": m.group(1)}, open(sys.argv[1], "w"))
EOF
	EXTRA_SOURCES=(patches/*.patch)
fi

mkdir -p "$TOP/SOURCES" "$TOP/SPECS"
tar -czf "$TOP/SOURCES/$TARBALL" -C "$STAGE" "fnotify-$V"
cp fnotify.spec "$TOP/SPECS/"
[[ ${#EXTRA_SOURCES[@]} -gt 0 ]] && cp "${EXTRA_SOURCES[@]}" "$TOP/SOURCES/"
rpmbuild -bs --define "_topdir $TOP" "$TOP/SPECS/fnotify.spec"
