// Copyright (c) 2026 Jia Chao
// fnotify is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

//! fnmatch 风格的通配符匹配，匹配对象是文件名（路径最后一段）。
//!
//! 支持：`*`（任意长度，不跨 `/`）、`?`（单字符）、`[abc]`、`[a-z]`、`[!abc]`（取反）。
//! 不做 shell 隐含规则：`.` 开头的文件名可被 `*` 匹配；未闭合的 `[` 按字面量处理。

/// 判断 `text` 是否匹配 `pattern`。
pub fn wildmatch(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    // 最近一次回溯点：star_p = '*' 在 p 中的下标，star_t = '*' 已匹配到 t 的下标。
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);
    while ti < t.len() {
        // '*' 必须先于 match_token 判断，否则会被当作字面量消费掉。
        if pi < p.len() && p[pi] == '*' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if pi < p.len() && match_token(&p, &mut pi, t[ti]) {
            ti += 1;
        } else if star_p != usize::MAX && t[star_t] != '/' {
            // 当前位置失配，回溯：让最近的 '*' 多吞一个字符（不许跨 '/'）。
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// 匹配并跳过 p[pi] 处的一个模式 token（`?`、字符类或字面字符）。
/// 越界访问由调用方的 `pi < p.len()` 保证不会发生。
fn match_token(p: &[char], pi: &mut usize, c: char) -> bool {
    match p[*pi] {
        '?' => {
            *pi += 1;
            true
        }
        '[' => match match_class(p, *pi, c) {
            Some((hit, end)) => {
                *pi = end + 1; // end 指向闭合 ']'
                hit
            }
            None => {
                // 未闭合的 '[' 视为字面量。
                *pi += 1;
                c == '['
            }
        },
        ch => {
            *pi += 1;
            ch == c
        }
    }
}

/// 匹配从 p[start]（为 '['）开始的字符类。
/// 返回 `Some((是否命中, 闭合 ']' 的下标))`；未闭合返回 `None`。
fn match_class(p: &[char], start: usize, c: char) -> Option<(bool, usize)> {
    let mut i = start + 1;
    let negate = i < p.len() && (p[i] == '!' || p[i] == '^');
    if negate {
        i += 1;
    }
    let mut hit = false;
    let mut first = true; // 紧跟在 '['（或取反符）后的 ']' 是字面量
    while i < p.len() && (p[i] != ']' || first) {
        first = false;
        if i + 2 < p.len() && p[i + 1] == '-' && p[i + 2] != ']' {
            if p[i] <= c && c <= p[i + 2] {
                hit = true;
            }
            i += 3;
        } else {
            if p[i] == c {
                hit = true;
            }
            i += 1;
        }
    }
    if i >= p.len() {
        return None;
    }
    Some((hit != negate, i))
}

#[cfg(test)]
mod tests {
    use super::wildmatch;

    #[test]
    fn star_matches_any_suffix() {
        assert!(wildmatch("*.log", "a.log"));
        assert!(wildmatch("*.log", "events-2026.log"));
        assert!(!wildmatch("*.log", "a.txt"));
        assert!(wildmatch("a*b*c", "aXbYc")); // 多个 '*' 回溯
    }

    #[test]
    fn star_does_not_cross_slash() {
        assert!(!wildmatch("*", "a/b"));
        assert!(wildmatch("*/*", "a/b"));
    }

    #[test]
    fn question_mark_matches_single_char() {
        assert!(wildmatch("?.log", "x.log"));
        assert!(!wildmatch("?.log", "ab.log"));
        assert!(wildmatch("??.log", "ab.log"));
    }

    #[test]
    fn char_class_membership() {
        assert!(wildmatch("[abc].log", "b.log"));
        assert!(!wildmatch("[abc].log", "d.log"));
        assert!(wildmatch("[a-z].log", "q.log"));
        assert!(!wildmatch("[a-z].log", "Q.log"));
        assert!(wildmatch("[]a].log", "].log")); // 首位 ']' 是字面量
    }

    #[test]
    fn char_class_negation() {
        assert!(wildmatch("[!abc].log", "d.log"));
        assert!(!wildmatch("[!abc].log", "a.log"));
        assert!(!wildmatch("[!a-z].log", "g.log"));
    }

    #[test]
    fn empty_pattern_only_matches_empty() {
        assert!(wildmatch("", ""));
        assert!(!wildmatch("", "a"));
    }

    #[test]
    fn unicode_names() {
        assert!(wildmatch("*.日志", "事件.日志"));
        assert!(wildmatch("事*", "事件.日志"));
        assert!(!wildmatch("*.日志", "事件.log"));
    }

    #[test]
    fn dotfiles_match_by_default() {
        assert!(wildmatch("*", ".hidden"));
        assert!(wildmatch("*.conf", ".ssh.conf"));
    }

    #[test]
    fn literal_and_trailing_star() {
        assert!(wildmatch("abc", "abc"));
        assert!(wildmatch("abc*", "abcdef"));
        assert!(!wildmatch("abc*", "ab"));
        assert!(!wildmatch("abc", "abcd"));
    }

    #[test]
    fn unclosed_bracket_is_literal() {
        assert!(wildmatch("a[b", "a[b"));
        assert!(!wildmatch("a[b", "ab"));
    }

    /// 边界行为固定测试：以下断言描述当前实现的实际行为，而非规范承诺；
    /// 将来修改实现时应是有意为之，并同步更新断言。
    #[test]
    fn edge_cases_pinned_to_current_behavior() {
        // [^abc]：'^' 与 '!' 同义取反（fnmatch 风格）。
        assert!(wildmatch("[^abc].log", "d.log"));
        assert!(!wildmatch("[^abc].log", "a.log"));
        // '**' 与 '*' 等价（连续星折叠为贪婪吞并），同样不跨 '/'。
        assert!(wildmatch("**", "abc"));
        assert!(wildmatch("a**b", "ab"));
        assert!(!wildmatch("**", "a/b"));
        // [a-]：'-' 位于末位按字面量，集合 = {a, -}。
        assert!(wildmatch("[a-].log", "a.log"));
        assert!(wildmatch("[a-].log", "-.log"));
        assert!(!wildmatch("[a-].log", "b.log"));
        // []]：首位 ']' 是字面量，[]] 只匹配 ']'。
        assert!(wildmatch("[]]", "]"));
        assert!(!wildmatch("[]]", "a"));
        // 反向范围 [z-a]：空区间永不命中；取反后全通过。
        assert!(!wildmatch("[z-a].log", "q.log"));
        assert!(!wildmatch("[z-a].log", "z.log"));
        assert!(!wildmatch("[z-a].log", "a.log"));
        assert!(wildmatch("[!z-a].log", "q.log"));
        // '?' 按 Unicode 字符（非字节）计：多字节字符算一个。
        assert!(wildmatch("?.log", "事.log"));
        assert!(wildmatch("??", "事件"));
        assert!(!wildmatch("?.log", "事件.log"));
    }
}
