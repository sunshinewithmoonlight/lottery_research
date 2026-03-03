#!/usr/bin/env python3
"""Collect folk tips about SSQ (双色球) from Google/Bing/Sogou/DuckDuckGo.

Outputs:
- raw jsonl/csv
- deduplicated master csv/json
- staged exploration report (markdown)
- detailed experience handbook (markdown)
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import html
import json
import random
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

# Local skill path provided by user.
SAFARI_SKILL_SCRIPTS = "/Users/shine/Downloads/skills/webview-assistant/scripts"
if SAFARI_SKILL_SCRIPTS not in sys.path:
    sys.path.append(SAFARI_SKILL_SCRIPTS)

from safari_ops import SafariOps  # type: ignore


QUERY_LIST = [
    "双色球 技巧",
    "双色球 心得",
    "双色球 经验",
    "双色球 选号 技巧",
    "双色球 投注 技巧",
    "双色球 走势 技巧",
    "双色球 冷热号 技巧",
    "双色球 胆拖 技巧",
    "双色球 蓝球 技巧",
    "双色球 红球 技巧",
    "双色球 和值 技巧",
    "双色球 奇偶 技巧",
    "双色球 连号 技巧",
    "双色球 杀号 技巧",
    "双色球 守号 经验",
    "双色球 复式 技巧",
    "双色球 机选 经验",
    "双色球 实战 技巧",
    "双色球 老彩民 经验",
    "双色球 中奖 心得",
    "双色球 精华 帖",
    "双色球 论坛 心得",
    "双色球 彩民 交流",
    "双色球 大底 技巧",
    "双色球 定位 法",
    "双色球 断组 技巧",
    "双色球 分析 方法",
    "双色球 历史 规律",
    "福彩 双色球 技巧",
    "中国 福彩 双色球 心得",
    "福彩 双色球 选号",
    "福彩 双色球 投注 经验",
    "双色球 蓝球 定位",
    "双色球 红球 分区",
    "双色球 跨度 技巧",
    "双色球 胆码 拖码",
    "双色球 预算 管理",
    "双色球 止损 策略",
    "双色球 复盘 方法",
    "双色球 人工 选号",
    "双色球 数据 选号",
    "双色球 趋势 图 技巧",
    "双色球 书籍",
    "双色球 选号 书",
    "双色球 技巧 图书",
    "双色球 彩票 书籍 推荐",
    "双色球 概率 书",
    "双色球 电子书",
    "双色球 PDF",
    "双色球 作者 出版社",
    "双色球 教程",
    "双色球 选号 手册",
    "双色球 博客 技巧",
    "site:zhihu.com 双色球 技巧",
    "site:tieba.baidu.com 双色球 心得",
    "site:sohu.com 双色球 技巧",
    "site:163.com 双色球 技巧",
    "site:zhcw.com 双色球 选号",
    "site:cwl.gov.cn 双色球",
    "site:500.com 双色球 选号",
    "双色球 龙头 凤尾 技巧",
    "双色球 数学 模型 选号",
    "双色球 期号 分析",
    "双色球 蓝球 预测 经验",
    "双色球 单式 复式 对比",
    "双色球 概率 统计 经验",
    "双色球 选号 心法",
    "双色球 彩经 技巧",
    "双色球 号码 分布",
    "双色球 历史 回测",
    "双色球 买彩 心态",
    "双色球 二次筛选 技巧",
    "双色球 投注 方案",
    "双色球 低成本 玩法",
    "双色球 中奖 复盘",
    "双色球 选号 教学",
    "双色球 彩民 笔记",
    "双色球 避坑 指南",
    "双色球 预算 纪律",
    "双色球 风险 控制",
]

LOTTERY_KEYWORDS = {
    "双色球",
    "ssq",
    "福彩",
    "彩票",
    "彩民",
    "选号",
    "红球",
    "蓝球",
    "胆拖",
    "杀号",
    "和值",
    "连号",
    "奇偶",
    "走势",
    "中奖",
}

BOOK_POSITIVE = {
    "书籍",
    "图书",
    "电子书",
    "pdf",
    "isbn",
    "出版社",
    "作者",
    "书名",
    "教程",
    "手册",
    "宝典",
    "book",
}

BOOK_NEGATIVE = {
    "图书馆",
    "个人图书馆",
}

FORUM_KEYWORDS = {
    "论坛",
    "贴吧",
    "问答",
    "社区",
    "知乎",
    "经验",
    "心得",
    "实战",
    "攻略",
}

TRACKING_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "spm",
    "from",
    "source",
    "s_from",
    "rsv_idx",
    "rsv_pq",
    "rsv_t",
    "eqid",
    "form",
    "mkt",
    "setlang",
    "count",
    "first",
    "rdr",
    "rdrig",
    "ved",
    "usg",
    "sa",
    "ei",
    "oq",
    "aqs",
    "hl",
    "gws_rd",
    "si",
    "fr",
    "f",
    "ig",
    "sid",
    "pc",
    "pno",
    "clk",
}
TRACKING_PREFIXES = ("utm_",)


@dataclass
class ResultRow:
    engine: str
    query: str
    rank: int
    title: str
    url: str
    canonical_url: str
    snippet: str
    fetched_at: str
    related: bool


def now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def strip_tags(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def norm_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def canonicalize_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    url = html.unescape(raw_url.strip())
    if url.startswith("//"):
        url = "https:" + url

    p = urlparse(url)
    if p.scheme not in {"http", "https"}:
        return ""

    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    path = p.path or "/"
    if len(path) > 1:
        path = path.rstrip("/")

    filtered_q = []
    for k, v in parse_qs(p.query, keep_blank_values=True).items():
        lk = k.lower()
        if lk in TRACKING_KEYS or any(lk.startswith(pref) for pref in TRACKING_PREFIXES):
            continue
        for vv in v:
            filtered_q.append((k, vv))

    query = urlencode(sorted(filtered_q), doseq=True)
    return urlunparse((p.scheme, host, path, "", query, ""))


def decode_google_url(url: str) -> str:
    p = urlparse(url)
    host = p.netloc.lower()
    if "google." in host and p.path == "/url":
        qs = parse_qs(p.query)
        for key in ("q", "url"):
            val = qs.get(key, [""])[0]
            if val.startswith("http"):
                return val
    return url


def decode_ddg_url(url: str) -> str:
    p = urlparse(url)
    if p.path == "/l/":
        qs = parse_qs(p.query)
        val = qs.get("uddg", [""])[0]
        if val.startswith("http"):
            return val
    return url


def decode_sogou_url(url: str) -> str:
    p = urlparse(url)
    qs = parse_qs(p.query)
    val = qs.get("url", [""])[0]
    if val.startswith("http"):
        return val
    return url


def decode_bing_redirect(url: str) -> str:
    p = urlparse(url)
    if "bing.com" not in p.netloc.lower() or not p.path.startswith("/ck/"):
        return url

    qs = parse_qs(p.query)
    token = qs.get("u", [""])[0]
    if not token:
        return url

    # Typical shape: a1aHR0cHM6Ly9leGFtcGxlLmNvbS8...
    if token.startswith("a1"):
        token = token[2:]

    pad = "=" * ((4 - len(token) % 4) % 4)
    try:
        decoded = base64.urlsafe_b64decode(token + pad).decode("utf-8", errors="ignore")
    except Exception:
        return url
    if decoded.startswith("http"):
        return decoded
    return url


def is_related(title: str, url: str, snippet: str, query: str) -> bool:
    text = f"{title} {url} {snippet}".lower()
    if any(k in text for k in LOTTERY_KEYWORDS):
        return True

    # Lightweight fallback: keep rows that explicitly include high-signal query tokens.
    high_signal = []
    for tok in re.split(r"\s+", query.lower().strip()):
        tok = tok.strip()
        if tok in {"双色球", "福彩", "彩票", "ssq"}:
            high_signal.append(tok)
    return any(tok in text for tok in high_signal)


def classify_category(title: str, url: str, snippet: str, query: str) -> str:
    text = f"{title} {url} {snippet} {query}".lower()
    if any(k in text for k in BOOK_NEGATIVE):
        book_flag = False
    else:
        book_flag = any(k in text for k in BOOK_POSITIVE)

    if book_flag:
        return "book"
    if any(k in text for k in FORUM_KEYWORDS):
        return "forum"
    return "article"


class SafariCollector:
    def __init__(self, wait_seconds: float = 4.5):
        self.ops = SafariOps()
        self.wait_seconds = wait_seconds
        self.anchor_js = (
            "JSON.stringify(Array.from(document.querySelectorAll('a'))"
            ".map(function(a){return {text:(a.innerText||'').trim(),href:(a.href||'')};})"
            ".slice(0,700))"
        )

    def _extract_anchors(self) -> List[Dict[str, str]]:
        raw = self.ops.execute_js(self.anchor_js)
        if not raw:
            return []
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [d for d in data if isinstance(d, dict)]
        except json.JSONDecodeError:
            return []
        return []

    def _keep_engine_link(self, engine: str, title: str, url: str) -> bool:
        if not url.startswith("http"):
            return False
        if len(title.strip()) < 2:
            return False
        host = (urlparse(url).netloc or "").lower()

        blocked_hosts = {
            "google": ("google.com", "gstatic.com", "googleusercontent.com"),
            "duckduckgo": ("duckduckgo.com",),
        }
        for bh in blocked_hosts[engine]:
            if bh in host:
                return False

        # Skip obvious navigation strings.
        if title.strip() in {"下一页", "上一页", "全部", "图片", "视频", "地图", "新闻", "更多"}:
            return False

        return True

    def collect_google(self, query: str) -> List[Dict[str, str]]:
        search_url = f"https://www.google.com/search?q={quote(query)}&num=30&hl=zh-CN"
        self.ops.navigate_to(search_url)
        time.sleep(self.wait_seconds)

        anchors = self._extract_anchors()
        results: List[Dict[str, str]] = []
        seen = set()
        rank = 0
        for item in anchors:
            title = norm_space(item.get("text", ""))
            url = decode_google_url(item.get("href", ""))
            url = canonicalize_url(url)
            if not self._keep_engine_link("google", title, url):
                continue
            if url in seen:
                continue
            seen.add(url)
            rank += 1
            results.append({"title": title, "url": url, "snippet": "", "rank": rank})
        return results

    def collect_duckduckgo(self, query: str) -> List[Dict[str, str]]:
        search_url = f"https://duckduckgo.com/?q={quote(query)}&ia=web"
        self.ops.navigate_to(search_url)
        time.sleep(self.wait_seconds)

        anchors = self._extract_anchors()
        results: List[Dict[str, str]] = []
        seen = set()
        rank = 0
        for item in anchors:
            title = norm_space(item.get("text", ""))
            url = decode_ddg_url(item.get("href", ""))
            url = canonicalize_url(url)
            if not self._keep_engine_link("duckduckgo", title, url):
                continue
            if url in seen:
                continue
            seen.add(url)
            rank += 1
            results.append({"title": title, "url": url, "snippet": "", "rank": rank})
        return results


class HttpCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
        )

    def _get(self, url: str, timeout: int = 35, retries: int = 2) -> str:
        last_exc: Optional[Exception] = None
        for _ in range(retries + 1):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp.text
            except Exception as exc:  # pragma: no cover - network variability
                last_exc = exc
            time.sleep(1.2 + random.random() * 0.8)
        if last_exc:
            raise last_exc
        return ""

    def collect_bing(self, query: str) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        seen = set()
        rank = 0

        search_queries = [query]
        if "福彩" not in query and "彩票" not in query:
            search_queries.append(f"福彩 {query}")

        for q in search_queries:
            search_url = (
                "https://cn.bing.com/search?"
                + urlencode({"q": q, "setlang": "zh-Hans", "mkt": "zh-CN", "count": "50"})
            )
            html_text = self._get(search_url)
            soup = BeautifulSoup(html_text, "lxml")

            items = soup.select("li.b_algo")
            if not items:
                items = soup.select("h2")

            for node in items:
                a = node.select_one("h2 a") if node.name != "h2" else node.select_one("a")
                if not a:
                    continue
                title = norm_space(a.get_text(" ", strip=True))
                href = decode_bing_redirect(a.get("href", ""))
                href = canonicalize_url(href)
                if not href.startswith("http"):
                    continue
                host = (urlparse(href).netloc or "").lower()
                if "bing.com" in host:
                    continue
                if href in seen:
                    continue
                seen.add(href)
                snippet_node = node.select_one("p") if node.name != "h2" else None
                snippet = norm_space(snippet_node.get_text(" ", strip=True)) if snippet_node else ""
                rank += 1
                rows.append({"title": title, "url": href, "snippet": snippet, "rank": rank})

        return rows

    def collect_sogou(self, query: str) -> List[Dict[str, str]]:
        search_url = "https://m.sogou.com/web/searchList.jsp?" + urlencode({"keyword": query})
        html_text = self._get(search_url)
        soup = BeautifulSoup(html_text, "lxml")

        rows: List[Dict[str, str]] = []
        seen = set()
        rank = 0

        # Primary source: embedded per-card JSON.
        for script in soup.select('script[id^="data-"][type="application/json"]'):
            raw = (script.string or script.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if not isinstance(data, dict):
                continue
            title = strip_tags(str(data.get("title", "")))
            href = decode_sogou_url(str(data.get("url", "")))
            href = canonicalize_url(href)
            if not href.startswith("http"):
                continue
            if href in seen:
                continue
            seen.add(href)
            snippet = strip_tags(str(data.get("content", "")))
            rank += 1
            rows.append({"title": title, "url": href, "snippet": snippet, "rank": rank})

        # Fallback for cards where only tc-link keeps final url in query param.
        for a in soup.select("a[href*='url=']"):
            title = norm_space(a.get_text(" ", strip=True))
            href = decode_sogou_url(a.get("href", ""))
            href = canonicalize_url(href)
            if not href.startswith("http"):
                continue
            if href in seen:
                continue
            if len(title) < 2:
                continue
            seen.add(href)
            rank += 1
            rows.append({"title": title, "url": href, "snippet": "", "rank": rank})

        return rows


def write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: Iterable[dict], fields: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            out = {k: row.get(k, "") for k in fields}
            writer.writerow(out)


def build_phase_report(
    path: Path,
    run_start: str,
    run_end: str,
    query_count: int,
    stats: Dict[str, Dict[str, int]],
    raw_rows: List[dict],
    unique_rows: List[dict],
    book_rows: List[dict],
) -> None:
    domain_counter = Counter(urlparse(r["canonical_url"]).netloc for r in unique_rows if r.get("canonical_url"))
    top_domains = domain_counter.most_common(40)

    unique_by_engine = Counter()
    for row in unique_rows:
        for e in row.get("engines", "").split(";"):
            if e:
                unique_by_engine[e] += 1

    lines = []
    lines.append("# 双色球坊间技巧探索阶段报告")
    lines.append("")
    lines.append(f"- 任务时间: {run_start} 至 {run_end}")
    lines.append(f"- 查询词规模: {query_count} 条")
    lines.append("- 搜索引擎: Google / Bing / 搜狗 / DuckDuckGo")
    lines.append("")
    lines.append("## 0. Safari技能测试结论")
    lines.append("")
    lines.append("- 路径 `/Users/shine/Downloads/skills/webview-assistant` 可执行。")
    lines.append("- `get_url` / `navigate` / `get_text` / `get_source` 正常。")
    lines.append("- 启用“来自 Apple 事件的 JavaScript”后，`execute_js` 返回正常，可用于页面级补充提取。")
    lines.append("")
    lines.append("## 1. 数据规模")
    lines.append("")
    lines.append(f"- 原始采集结果: {len(raw_rows)} 条")
    lines.append(f"- 去重后唯一链接: {len(unique_rows)} 条")
    lines.append(f"- 疑似书籍/资料线索: {len(book_rows)} 条")
    lines.append("")
    lines.append("### 1.1 分引擎统计")
    lines.append("")
    lines.append("| 引擎 | 原始条数 | 关键词过滤后 | 去重后贡献 |")
    lines.append("|---|---:|---:|---:|")
    for engine in ["google", "bing", "sogou", "duckduckgo"]:
        raw = stats[engine]["raw"]
        rel = stats[engine]["related"]
        uni = unique_by_engine.get(engine, 0)
        lines.append(f"| {engine} | {raw} | {rel} | {uni} |")
    lines.append("")

    lines.append("### 1.2 高频站点 (Top 40)")
    lines.append("")
    for d, c in top_domains:
        lines.append(f"- {d}: {c}")
    lines.append("")

    lines.append("## 2. 观察到的内容特征")
    lines.append("")
    lines.append("- 坊间技巧主要集中在: 冷热号、奇偶比、和值区间、连号/重号、蓝球定位、胆拖复式。")
    lines.append("- 经验帖来源以社区/自媒体/彩票资讯站为主，质量差异大。")
    lines.append("- 含“必中/稳赚/99%准确”等表述的内容占比不低，风险提示必要。")
    lines.append("- 书籍线索既有正式出版物，也有二次转载页面，需二次核验 ISBN/出版社。")
    lines.append("")

    lines.append("## 3. 书籍线索样本 (前 120 条内精选)")
    lines.append("")
    for row in book_rows[:120]:
        lines.append(f"- [{row['title'] or row['canonical_url']}]({row['canonical_url']})")
    lines.append("")

    lines.append("## 4. 下一阶段扩容建议")
    lines.append("")
    lines.append("- 对高价值来源做二次抓取: 进入正文提炼“可执行步骤 + 反例 + 风险边界”。")
    lines.append("- 引入时间维度: 分近一年/历史帖，识别过时技巧。")
    lines.append("- 书籍线索做元数据清洗: 统一书名、作者、ISBN、出版社、发行年。")
    lines.append("- 建立“误导词”审查标签: 必中、包中、稳赚等。")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def pick_evidence(unique_rows: List[dict], keywords: List[str], limit: int = 10) -> List[dict]:
    out = []
    seen = set()
    for row in unique_rows:
        text = f"{row.get('title','')} {row.get('query','')} {row.get('snippet','')}".lower()
        if not any(k in text for k in keywords):
            continue
        u = row.get("canonical_url", "")
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(row)
        if len(out) >= limit:
            break
    return out


def build_handbook(path: Path, unique_rows: List[dict], book_rows: List[dict]) -> None:
    sections = [
        {
            "title": "01. 冷热号搭配",
            "keywords": ["冷号", "热号", "冷热"],
            "ops": [
                "滚动观察 30-100 期出现频次，避免极端全热或全冷。",
                "保留 1-2 个相对冷号，其余用中频号平衡。",
                "每期记录命中情况，连续偏离时调参。",
            ],
            "risk": "把“冷号该出”当成必然是典型误区，开奖独立随机。",
        },
        {
            "title": "02. 奇偶比与大小比",
            "keywords": ["奇偶", "大小", "比例"],
            "ops": [
                "常见组合作为覆盖基线（如 3:3 / 2:4 / 4:2）。",
                "不要长期固定单一比例。",
                "与和值区间联动，而不是孤立看比例。",
            ],
            "risk": "比例法只能约束结构，不能直接提高单注命中概率。",
        },
        {
            "title": "03. 和值区间",
            "keywords": ["和值", "区间"],
            "ops": [
                "先统计近 50-100 期和值分布，再设当前候选区间。",
                "极低和值与极高和值组合适当降权。",
                "与跨度、连号一起复核。",
            ],
            "risk": "历史区间并非未来保证，区间漂移会发生。",
        },
        {
            "title": "04. 连号与重号",
            "keywords": ["连号", "重号", "邻号"],
            "ops": [
                "每注留 0-2 组连号的弹性位，不要机械必选。",
                "重号策略以“近一期到三期”做弱约束。",
                "当连号连续失效时缩小连号权重。",
            ],
            "risk": "将连号当“硬规则”会导致覆盖面失衡。",
        },
        {
            "title": "05. 蓝球优先法",
            "keywords": ["蓝球", "后区"],
            "ops": [
                "把蓝球当作单独子问题，维护小池（如 3-5 个）。",
                "结合遗漏与近期热度动态换池。",
                "复式方案优先扩展蓝球而非无限扩红球。",
            ],
            "risk": "蓝球池过大等于回到盲打，预算会快速膨胀。",
        },
        {
            "title": "06. 胆拖与复式",
            "keywords": ["胆拖", "复式", "大底"],
            "ops": [
                "胆码控制在 1-2 个，拖码避免过长。",
                "复式只在高置信结构下使用，避免常态重注。",
                "设定单期硬预算上限。",
            ],
            "risk": "胆错会整体失效，复式提升覆盖但不改变随机本质。",
        },
        {
            "title": "07. 守号与机选混合",
            "keywords": ["守号", "机选", "随机"],
            "ops": [
                "守号保留少量长期组合，机选用于对冲主观偏差。",
                "给守号设评估窗口（如 20 期）并定期淘汰。",
                "混合比例固定后少频繁摇摆。",
            ],
            "risk": "只守号或只追热都容易陷入认知偏误。",
        },
        {
            "title": "08. 预算与止损",
            "keywords": ["预算", "止损", "倍投", "风险"],
            "ops": [
                "按月预算，不因连错加码追损。",
                "倍投仅在明确规则下使用，且设置上限。",
                "出现情绪化下单时立即暂停。",
            ],
            "risk": "追损是最常见亏损放大器。",
        },
        {
            "title": "09. 数据复盘流程",
            "keywords": ["复盘", "统计", "数据"],
            "ops": [
                "每期记录: 结构命中、蓝球命中、预算执行。",
                "每 10-20 期复盘一次，淘汰无效规则。",
                "区分“偶然命中”与“可重复策略”。",
            ],
            "risk": "只看中奖样本会产生幸存者偏差。",
        },
        {
            "title": "10. 书籍与系统学习",
            "keywords": ["书籍", "图书", "教程", "手册", "出版社", "isbn"],
            "ops": [
                "优先找可核验元数据（书名/作者/出版社/ISBN）。",
                "区分正式出版与转载摘录。",
                "用书中方法做小样本回测再纳入策略。",
            ],
            "risk": "“电子书汇编/帖子拼接”常有过度承诺与错误归纳。",
        },
        {
            "title": "11. 信息源分级",
            "keywords": ["论坛", "知乎", "经验", "攻略", "专家"],
            "ops": [
                "把来源分为 A/B/C 级: 官方数据站 > 方法论文章 > 口号式帖子。",
                "高频传播但无过程数据的内容降低权重。",
                "同一技巧至少在 3 个来源交叉验证。",
            ],
            "risk": "单一来源强信念会导致错误策略固化。",
        },
        {
            "title": "12. 反诈骗与合规提示",
            "keywords": ["必中", "包中", "稳赚", "内幕", "导师"],
            "ops": [
                "遇到“包中/带单收费/内幕号码”直接排除。",
                "不向陌生渠道转账购买所谓预测服务。",
                "始终把彩票视为娱乐预算，不借贷投注。",
            ],
            "risk": "高收益承诺几乎总伴随诈骗或严重误导。",
        },
    ]

    lines = []
    lines.append("# 双色球坊间技巧经验条目手册")
    lines.append("")
    lines.append("- 说明: 本手册基于多搜索引擎公开页面汇总，目标是“整理经验”，不是保证中奖。")
    lines.append("- 原则: 开奖独立随机，任何技巧都只能用于结构化决策与风险控制。")
    lines.append("")

    for sec in sections:
        lines.append(f"## {sec['title']}")
        lines.append("")
        lines.append("- 操作要点:")
        for op in sec["ops"]:
            lines.append(f"  - {op}")
        lines.append(f"- 风险提示: {sec['risk']}")

        evidence = pick_evidence(unique_rows, sec["keywords"], limit=10)
        if evidence:
            lines.append("- 参考线索:")
            for row in evidence:
                title = row.get("title") or row.get("canonical_url")
                lines.append(f"  - [{title}]({row.get('canonical_url')})")
        lines.append("")

    lines.append("## 附录A: 书籍/资料线索清单")
    lines.append("")
    for row in book_rows[:200]:
        title = row.get("title") or row.get("canonical_url")
        lines.append(f"- [{title}]({row.get('canonical_url')})")
    lines.append("")

    lines.append("## 附录B: 使用方式建议")
    lines.append("")
    lines.append("- 先定预算，再选结构，不因单期输赢改规则。")
    lines.append("- 每期只执行一套固定流程，保留复盘记录。")
    lines.append("- 对“必中类表述”一律降为噪声信息。")

    path.write_text("\n".join(lines), encoding="utf-8")


def run(output_root: Path, max_queries: Optional[int], safari_wait: float) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    data_dir = output_root / "data"
    reports_dir = output_root / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    queries = QUERY_LIST[: max_queries or len(QUERY_LIST)]

    safari = SafariCollector(wait_seconds=safari_wait)
    http_collector = HttpCollector()

    stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"raw": 0, "related": 0})
    raw_rows: List[dict] = []

    run_start = now_ts()

    for idx, query in enumerate(queries, start=1):
        print(f"[{idx}/{len(queries)}] Query: {query}")

        # Google via Safari
        try:
            g_rows = safari.collect_google(query)
        except Exception as exc:  # pragma: no cover - environment variability
            print(f"  - google error: {exc}")
            g_rows = []
        stats["google"]["raw"] += len(g_rows)
        rel = 0
        for row in g_rows:
            related = is_related(row["title"], row["url"], row["snippet"], query)
            if related:
                rel += 1
            raw_rows.append(
                ResultRow(
                    engine="google",
                    query=query,
                    rank=row["rank"],
                    title=row["title"],
                    url=row["url"],
                    canonical_url=canonicalize_url(row["url"]),
                    snippet=row["snippet"],
                    fetched_at=now_ts(),
                    related=related,
                ).__dict__
            )
        stats["google"]["related"] += rel

        # DuckDuckGo via Safari
        try:
            d_rows = safari.collect_duckduckgo(query)
        except Exception as exc:  # pragma: no cover - environment variability
            print(f"  - duckduckgo error: {exc}")
            d_rows = []
        stats["duckduckgo"]["raw"] += len(d_rows)
        rel = 0
        for row in d_rows:
            related = is_related(row["title"], row["url"], row["snippet"], query)
            if related:
                rel += 1
            raw_rows.append(
                ResultRow(
                    engine="duckduckgo",
                    query=query,
                    rank=row["rank"],
                    title=row["title"],
                    url=row["url"],
                    canonical_url=canonicalize_url(row["url"]),
                    snippet=row["snippet"],
                    fetched_at=now_ts(),
                    related=related,
                ).__dict__
            )
        stats["duckduckgo"]["related"] += rel

        # Bing via HTTP
        try:
            b_rows = http_collector.collect_bing(query)
        except Exception as exc:  # pragma: no cover - environment variability
            print(f"  - bing error: {exc}")
            b_rows = []
        stats["bing"]["raw"] += len(b_rows)
        rel = 0
        for row in b_rows:
            related = is_related(row["title"], row["url"], row["snippet"], query)
            if related:
                rel += 1
            raw_rows.append(
                ResultRow(
                    engine="bing",
                    query=query,
                    rank=row["rank"],
                    title=row["title"],
                    url=row["url"],
                    canonical_url=canonicalize_url(row["url"]),
                    snippet=row["snippet"],
                    fetched_at=now_ts(),
                    related=related,
                ).__dict__
            )
        stats["bing"]["related"] += rel

        # Sogou via HTTP (mobile results)
        try:
            s_rows = http_collector.collect_sogou(query)
        except Exception as exc:  # pragma: no cover - environment variability
            print(f"  - sogou error: {exc}")
            s_rows = []
        stats["sogou"]["raw"] += len(s_rows)
        rel = 0
        for row in s_rows:
            related = is_related(row["title"], row["url"], row["snippet"], query)
            if related:
                rel += 1
            raw_rows.append(
                ResultRow(
                    engine="sogou",
                    query=query,
                    rank=row["rank"],
                    title=row["title"],
                    url=row["url"],
                    canonical_url=canonicalize_url(row["url"]),
                    snippet=row["snippet"],
                    fetched_at=now_ts(),
                    related=related,
                ).__dict__
            )
        stats["sogou"]["related"] += rel

        time.sleep(0.8 + random.random() * 0.4)

    related_rows = [r for r in raw_rows if r.get("related") and r.get("canonical_url")]

    # Deduplicate by canonical URL, merge engine/query provenance.
    merged: Dict[str, dict] = {}
    for row in related_rows:
        key = row["canonical_url"]
        if key not in merged:
            merged[key] = {
                "canonical_url": key,
                "title": row["title"],
                "snippet": row["snippet"],
                "first_seen": row["fetched_at"],
                "best_rank": row["rank"],
                "engines": {row["engine"]},
                "queries": {row["query"]},
            }
            continue

        m = merged[key]
        m["engines"].add(row["engine"])
        m["queries"].add(row["query"])
        if row["rank"] < m["best_rank"]:
            m["best_rank"] = row["rank"]
            if len(row["title"]) >= len(m["title"]):
                m["title"] = row["title"]
            if len(row["snippet"]) > len(m["snippet"]):
                m["snippet"] = row["snippet"]

    unique_rows: List[dict] = []
    for _, item in merged.items():
        query_str = ";".join(sorted(item["queries"]))
        category = classify_category(item["title"], item["canonical_url"], item["snippet"], query_str)
        unique_rows.append(
            {
                "canonical_url": item["canonical_url"],
                "title": item["title"],
                "snippet": item["snippet"],
                "category": category,
                "best_rank": item["best_rank"],
                "engines": ";".join(sorted(item["engines"])),
                "query": query_str,
                "first_seen": item["first_seen"],
            }
        )

    unique_rows.sort(key=lambda x: (x["best_rank"], x["canonical_url"]))
    book_rows = [r for r in unique_rows if r["category"] == "book"]

    run_end = now_ts()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_jsonl = data_dir / f"raw_results_{stamp}.jsonl"
    raw_csv = data_dir / f"raw_results_{stamp}.csv"
    uniq_json = data_dir / f"unique_results_{stamp}.json"
    uniq_csv = data_dir / f"unique_results_{stamp}.csv"
    books_csv = data_dir / f"book_candidates_{stamp}.csv"

    write_jsonl(raw_jsonl, raw_rows)
    write_csv(
        raw_csv,
        raw_rows,
        [
            "engine",
            "query",
            "rank",
            "title",
            "url",
            "canonical_url",
            "snippet",
            "fetched_at",
            "related",
        ],
    )
    uniq_json.write_text(json.dumps(unique_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(
        uniq_csv,
        unique_rows,
        [
            "canonical_url",
            "title",
            "snippet",
            "category",
            "best_rank",
            "engines",
            "query",
            "first_seen",
        ],
    )
    write_csv(
        books_csv,
        book_rows,
        [
            "canonical_url",
            "title",
            "snippet",
            "category",
            "best_rank",
            "engines",
            "query",
            "first_seen",
        ],
    )

    phase_report = reports_dir / f"phase_report_{stamp}.md"
    handbook = reports_dir / f"experience_handbook_{stamp}.md"

    build_phase_report(
        phase_report,
        run_start=run_start,
        run_end=run_end,
        query_count=len(queries),
        stats=stats,
        raw_rows=raw_rows,
        unique_rows=unique_rows,
        book_rows=book_rows,
    )
    build_handbook(handbook, unique_rows, book_rows)

    latest = {
        "raw_jsonl": str(raw_jsonl),
        "raw_csv": str(raw_csv),
        "unique_json": str(uniq_json),
        "unique_csv": str(uniq_csv),
        "book_csv": str(books_csv),
        "phase_report": str(phase_report),
        "handbook": str(handbook),
        "queries": len(queries),
        "raw_rows": len(raw_rows),
        "related_rows": len(related_rows),
        "unique_rows": len(unique_rows),
        "book_rows": len(book_rows),
        "stats": stats,
    }
    (output_root / "latest_run.json").write_text(
        json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print("\n=== DONE ===")
    print(json.dumps(latest, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect SSQ folk tips and generate reports.")
    parser.add_argument(
        "--output-root",
        default="/Users/shine/lottery_research",
        help="Output root directory.",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=None,
        help="Limit number of queries for test run.",
    )
    parser.add_argument(
        "--safari-wait",
        type=float,
        default=4.5,
        help="Seconds to wait after Safari navigation.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(output_root=Path(args.output_root), max_queries=args.max_queries, safari_wait=args.safari_wait)
