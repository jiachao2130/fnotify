# fnotify bash 补全：词首为 - 时补全 flag，否则补全路径。
# 清单与 `fnotify -h` 的选项集合对齐。
_fnotify() {
    local cur
    cur="${COMP_WORDS[COMP_CWORD]}"
    local flags="-r --recursive -g --glob --exclude -e --events
        --debounce --flush -x --exec --jobs --timeout --webhook
        --oneshot --until-idle --config -q --quiet
        -h --help -V --version"
    if [[ "$cur" == -* ]]; then
        COMPREPLY=($(compgen -W "$flags" -- "$cur"))
    else
        COMPREPLY=($(compgen -f -- "$cur"))
    fi
}
complete -o filenames -F _fnotify fnotify
