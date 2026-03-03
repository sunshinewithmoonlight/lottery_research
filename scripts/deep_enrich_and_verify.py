#!/usr/bin/env python3
"""Deep content crawl and book verification for SSQ research outputs.

Goals:
1) Crawl body-level content for unique links and generate evidence-paragraph handbook.
2) Verify/deduplicate book candidates and output trusted booklist with strong/medium/weak levels.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import random
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.parse import urlunparse

import requests
from bs4 import BeautifulSoup

# Optional Safari fallback for anti-bot pages (uses local skill).
SAFARI_SCRIPTS = "/Users/shine/Downloads/skills/webview-assistant/scripts"
if SAFARI_SCRIPTS not in sys.path:
    sys.path.append(SAFARI_SCRIPTS)
try:  # pragma: no cover - environment dependent
    from safari_ops import SafariOps  # type: ignore
except Exception:  # pragma: no cover
    SafariOps = None  # type: ignore

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

LOTTERY_SECTIONS = [
    {
        "id": "cold_hot",
        "title": "01. 冷热号搭配",
        "keywords": ["冷号", "热号", "冷热", "遗漏", "回补"],
    },
    {
        "id": "odd_even",
        "title": "02. 奇偶比与大小比",
        "keywords": ["奇偶", "大小比", "比例", "分布"],
    },
    {
        "id": "sum_zone",
        "title": "03. 和值区间",
        "keywords": ["和值", "区间", "跨度"],
    },
    {
        "id": "consecutive_repeat",
        "title": "04. 连号与重号",
        "keywords": ["连号", "重号", "邻号", "同尾"],
    },
    {
        "id": "blue_ball",
        "title": "05. 蓝球优先法",
        "keywords": ["蓝球", "后区", "独胆", "蓝码"],
    },
    {
        "id": "dantuo_fushi",
        "title": "06. 胆拖与复式",
        "keywords": ["胆拖", "复式", "大底", "缩水"],
    },
    {
        "id": "guard_mix",
        "title": "07. 守号与机选混合",
        "keywords": ["守号", "机选", "随机", "跟号"],
    },
    {
        "id": "budget_stoploss",
        "title": "08. 预算与止损",
        "keywords": ["预算", "止损", "倍投", "仓位", "资金管理"],
    },
    {
        "id": "replay",
        "title": "09. 数据复盘流程",
        "keywords": ["复盘", "回测", "统计", "样本"],
    },
    {
        "id": "book_learning",
        "title": "10. 书籍与系统学习",
        "keywords": ["书籍", "图书", "作者", "出版社", "isbn", "教程"],
    },
    {
        "id": "source_grading",
        "title": "11. 信息源分级",
        "keywords": ["论坛", "贴吧", "知乎", "社区", "经验帖", "权威"],
    },
    {
        "id": "anti_fraud",
        "title": "12. 反诈骗与合规提示",
        "keywords": ["必中", "包中", "稳赚", "内幕", "收费带单", "导师"],
    },
]

BOOK_SIGNAL = {
    "isbn",
    "出版社",
    "出版",
    "图书",
    "书籍",
    "书名",
    "作者",
    "book",
    "press",
    "publishing",
    "edition",
    "京东阅读",
    "当当",
    "豆瓣读书",
    "亚马逊",
}

BOOK_BAD_HOSTS = {
    "bilibili.com",
    "youtube.com",
    "v.qq.com",
    "toutiao.com",
}

BOOK_GOOD_HOSTS = {
    "abebooks.com",
    "amazon.com",
    "dangdang.com",
    "jd.com",
    "douban.com",
    "weread.qq.com",
    "baike.baidu.com",
    "worldcat.org",
    "isbnsearch.org",
    "book.douban.com",
}

BOOK_MARKET_HOSTS = {
    "taobao.com",
    "tmall.com",
    "suning.com",
    "hooos.com",
    "zazhi.com.cn",
}

REQUIRE_WWW_HOSTS = {
    "zhcw.com",
    "cpzj.com",
    "cwl.gov.cn",
    "00038.cn",
    "yclottery.com",
}


def now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text


def split_lines(text: str) -> List[str]:
    if not text:
        return []
    text = re.sub(r"[\t\r\f\v]+", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    lines = []
    seen = set()
    for raw in text.split("\n"):
        line = clean_text(raw)
        if len(line) < 12:
            continue
        key = line[:100]
        if key in seen:
            continue
        seen.add(key)
        lines.append(line)
    return lines


def normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u.startswith("http"):
        return u
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        if host in REQUIRE_WWW_HOSTS:
            p = p._replace(netloc="www." + host)
            return urlunparse(p)
    except Exception:
        pass
    return u


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def host_of(url: str) -> str:
    return (urlparse(url).netloc or "").lower()


def is_host_match(host: str, host_set: set) -> bool:
    h = (host or "").lower()
    return any(h == d or h.endswith("." + d) for d in host_set)


def build_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(
        {
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return sess


def fetch_url(session: requests.Session, url: str, timeout: int = 15, retries: int = 2) -> Tuple[int, str, str]:
    """Return status_code, final_url, html_text."""
    last_exc: Optional[Exception] = None
    for _ in range(retries + 1):
        ua = random.choice(USER_AGENTS)
        headers = {"User-Agent": ua}
        try:
            resp = session.get(url, timeout=timeout, headers=headers, allow_redirects=True)
            content_type = (resp.headers.get("content-type") or "").lower()
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                return resp.status_code, str(resp.url), ""
            text = resp.text or ""
            return resp.status_code, str(resp.url), text
        except Exception as exc:  # pragma: no cover
            last_exc = exc
            time.sleep(0.5 + random.random() * 0.8)
    raise RuntimeError(str(last_exc) if last_exc else "request failed")


def extract_main_text(html_text: str) -> Tuple[str, str, str]:
    """Return title, main_text, snippet."""
    if not html_text:
        return "", "", ""

    soup = BeautifulSoup(html_text, "lxml")
    for tag in soup(["script", "style", "noscript", "svg", "iframe", "header", "footer", "form"]):
        tag.decompose()

    title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""

    candidates = []
    selectors = [
        "article",
        "main",
        "div.article",
        "div.post",
        "div.content",
        "div#content",
        "section.article",
        "div[class*='article']",
        "div[class*='content']",
        "div[class*='post']",
    ]
    for sel in selectors:
        for node in soup.select(sel):
            txt = clean_text(node.get_text("\n", strip=True))
            if len(txt) >= 120:
                candidates.append(txt)

    if candidates:
        main_text = max(candidates, key=len)
    else:
        body = soup.body or soup
        main_text = clean_text(body.get_text("\n", strip=True))

    lines = split_lines(main_text)
    if lines:
        main_text = "\n".join(lines[:180])
    else:
        main_text = ""

    snippet = clean_text(main_text[:280])
    return title, main_text, snippet


def extract_evidence_paragraph(text: str, keywords: List[str]) -> str:
    if not text:
        return ""
    compact = clean_text(text)
    lower = compact.lower()

    pos = -1
    kw_hit = ""
    for kw in keywords:
        p = lower.find(kw.lower())
        if p != -1 and (pos == -1 or p < pos):
            pos = p
            kw_hit = kw

    if pos == -1:
        # fallback use head paragraph
        return compact[:180]

    start = max(0, pos - 70)
    end = min(len(compact), pos + 180)
    snip = compact[start:end]
    if start > 0:
        snip = "..." + snip
    if end < len(compact):
        snip = snip + "..."
    # Mark keyword once for readability if not obvious.
    if kw_hit and kw_hit not in snip:
        snip = f"[{kw_hit}] " + snip
    return snip


def isbn_checksum_valid(isbn: str) -> bool:
    s = re.sub(r"[^0-9Xx]", "", isbn)
    if len(s) == 13 and s.isdigit():
        total = sum((1 if i % 2 == 0 else 3) * int(ch) for i, ch in enumerate(s[:12]))
        check = (10 - (total % 10)) % 10
        return check == int(s[12])
    if len(s) == 10:
        total = 0
        for i, ch in enumerate(s[:9]):
            if not ch.isdigit():
                return False
            total += (10 - i) * int(ch)
        last = 10 if s[9] in {"X", "x"} else int(s[9]) if s[9].isdigit() else -1
        if last < 0:
            return False
        total += last
        return total % 11 == 0
    return False


def extract_isbns(text: str) -> List[str]:
    if not text:
        return []
    out = []
    patterns = [
        r"(?:ISBN(?:-1[03])?[:：\s]*)?([0-9]{3}[-\s]?[0-9][-\s]?[0-9]{2,6}[-\s]?[0-9]{2,7}[-\s]?[0-9Xx])",
        r"\b(97[89][0-9]{10})\b",
        r"\b([0-9]{9}[0-9Xx])\b",
    ]
    seen = set()
    for pat in patterns:
        for m in re.finditer(pat, text):
            v = re.sub(r"[^0-9Xx]", "", m.group(1))
            if len(v) in {10, 13} and v not in seen and isbn_checksum_valid(v):
                seen.add(v)
                out.append(v)
    return out


def extract_publishers(text: str) -> List[str]:
    if not text:
        return []
    pubs = set()
    for m in re.finditer(r"([\u4e00-\u9fa5A-Za-z·]{2,20}(?:出版社|出版集团|出版公司|大学出版社|Press|Publishing))", text):
        val = clean_text(m.group(1))
        val = re.sub(r"^[0-9一二三四五六七八九十年月\-\s]+", "", val)
        if 2 <= len(val) <= 30:
            if len(re.findall(r"出版社|Publishing|Press", val)) > 1:
                continue
            pubs.add(val)
    return sorted(pubs)


def extract_authors(text: str) -> List[str]:
    if not text:
        return []
    auth = set()
    # Chinese cues
    for m in re.finditer(r"(?:作者|编著|主编|著)[:：\s]{0,4}([\u4e00-\u9fa5·A-Za-z\s]{2,40})", text):
        v = clean_text(m.group(1))
        if 2 <= len(v) <= 40:
            auth.add(v)
    return sorted(auth)


def normalize_book_title(title: str) -> str:
    t = clean_text(title)
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"\b(?:amazon|abebooks|dangdang|京东|豆瓣|百度百科|bilibili)\b", "", t, flags=re.I)
    t = re.sub(r"[【\[（(].*?[】\])）)]", "", t)
    t = re.sub(r"[^\u4e00-\u9fa5A-Za-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t[:120]


def read_csv(path: Path) -> List[dict]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_jsonl(path: Path) -> List[dict]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_csv(path: Path, rows: Iterable[dict], fields: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def choose_latest_paths(root: Path) -> Tuple[Path, Path, Path]:
    latest = json.loads((root / "latest_run.json").read_text(encoding="utf-8"))
    return Path(latest["unique_csv"]), Path(latest["book_csv"]), root


def crawl_all(
    unique_rows: List[dict],
    out_dir: Path,
    workers: int,
    timeout: int,
    safari_fallback: bool = True,
    safari_fallback_max: int = 250,
    safari_wait: float = 3.0,
) -> Tuple[List[dict], dict]:
    session = build_session()
    tasks = []

    for row in unique_rows:
        url = normalize_url(row.get("canonical_url", ""))
        if not url.startswith("http"):
            continue
        tasks.append(
            {
                "url": url,
                "title_seed": row.get("title", ""),
                "category": row.get("category", ""),
                "query": row.get("query", ""),
                "engines": row.get("engines", ""),
                "best_rank": row.get("best_rank", ""),
            }
        )

    results: List[dict] = []
    status_counter = Counter()
    host_counter = Counter()

    def job(task: dict) -> dict:
        url = task["url"]
        record = {
            "url": url,
            "url_hash": url_hash(url),
            "title_seed": task.get("title_seed", ""),
            "category": task.get("category", ""),
            "query": task.get("query", ""),
            "engines": task.get("engines", ""),
            "best_rank": task.get("best_rank", ""),
            "fetched_at": now_ts(),
            "status": "failed",
            "http_status": "",
            "final_url": "",
            "title": "",
            "snippet": "",
            "text": "",
            "text_len": 0,
            "error": "",
        }
        try:
            code, final_url, html_text = fetch_url(session, url, timeout=timeout, retries=2)
            record["http_status"] = str(code)
            record["final_url"] = final_url
            if code >= 400:
                record["status"] = "http_error"
                return record
            if not html_text:
                record["status"] = "non_html"
                return record
            title, text, snippet = extract_main_text(html_text)
            record["title"] = title
            record["snippet"] = snippet
            record["text"] = text
            record["text_len"] = len(text)
            if len(text) >= 120:
                record["status"] = "ok"
            else:
                record["status"] = "thin"
            return record
        except Exception as exc:  # pragma: no cover
            record["error"] = str(exc)[:240]
            return record

    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut_map = {ex.submit(job, t): t for t in tasks}
        total = len(fut_map)
        done = 0
        for fut in as_completed(fut_map):
            done += 1
            rec = fut.result()
            results.append(rec)
            status_counter[rec["status"]] += 1
            host_counter[host_of(rec["url"])] += 1
            if done % 50 == 0 or done == total:
                print(f"crawl progress: {done}/{total} ok={status_counter['ok']} thin={status_counter['thin']} fail={status_counter['failed']}")

    safari_used = 0
    safari_hits = 0
    # Safari fallback for anti-bot domains where browser session is more likely to pass.
    if safari_fallback and SafariOps is not None and safari_fallback_max > 0:
        fallback_hosts = {"zhihu.com", "zhuanlan.zhihu.com", "tieba.baidu.com", "blog.csdn.net"}
        candidates = [
            r
            for r in results
            if r.get("status") in {"http_error", "failed", "thin"}
            and any(host_of(r.get("url", "")).endswith(h) or host_of(r.get("url", "")) == h for h in fallback_hosts)
        ]
        candidates = candidates[:safari_fallback_max]
        if candidates:
            print(f"safari fallback: {len(candidates)} candidates")
            ops = SafariOps()
            for i, rec in enumerate(candidates, 1):
                url = rec.get("url", "")
                if not url.startswith("http"):
                    continue
                safari_used += 1
                try:
                    ops.navigate_to(url)
                    time.sleep(safari_wait)
                    title = ops.execute_js("document.title") or rec.get("title", "")
                    page_text = ops.get_page_text() or ""
                    text = clean_text(page_text)
                    if len(text) >= 120:
                        # Roll up long text into paragraph-like lines.
                        lines = split_lines(text)
                        if lines:
                            text = "\n".join(lines[:180])
                        rec["title"] = clean_text(title)
                        rec["snippet"] = clean_text(text[:280])
                        rec["text"] = text
                        rec["text_len"] = len(text)
                        rec["status"] = "ok"
                        rec["http_status"] = rec.get("http_status") or "safari"
                        rec["final_url"] = ops.get_current_url() or rec.get("final_url", "")
                        rec["error"] = ""
                        safari_hits += 1
                except Exception as exc:  # pragma: no cover
                    rec["error"] = (rec.get("error", "") + f" | safari:{exc}")[:240]
                if i % 20 == 0:
                    print(f"safari fallback progress: {i}/{len(candidates)} recovered={safari_hits}")

            # Rebuild status counter after fallback updates.
            status_counter = Counter(r.get("status", "unknown") for r in results)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    jsonl_path = out_dir / f"deep_pages_{ts}.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for rec in results:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    csv_path = out_dir / f"deep_pages_{ts}.csv"
    write_csv(
        csv_path,
        results,
        [
            "url",
            "url_hash",
            "title_seed",
            "category",
            "query",
            "engines",
            "best_rank",
            "fetched_at",
            "status",
            "http_status",
            "final_url",
            "title",
            "snippet",
            "text_len",
            "error",
        ],
    )

    summary = {
        "deep_jsonl": str(jsonl_path),
        "deep_csv": str(csv_path),
        "total": len(results),
        "status": dict(status_counter),
        "safari_fallback_used": safari_used,
        "safari_fallback_recovered": safari_hits,
        "top_hosts": host_counter.most_common(30),
    }
    return results, summary


def build_evidence_handbook(results: List[dict], out_path: Path) -> dict:
    ok_rows = [r for r in results if r.get("status") == "ok" and r.get("text_len", 0) >= 120]

    # Section evidence candidates
    sec_evid: Dict[str, List[dict]] = {s["id"]: [] for s in LOTTERY_SECTIONS}
    for row in ok_rows:
        text = row.get("text", "")
        title = row.get("title") or row.get("title_seed") or row.get("url")
        url = row.get("final_url") or row.get("url")
        host = host_of(url)
        for sec in LOTTERY_SECTIONS:
            kws = sec["keywords"]
            if not any(kw.lower() in text.lower() for kw in kws):
                continue
            ev = extract_evidence_paragraph(text, kws)
            score = min(1000, len(text))
            # Slightly prioritize non-video/community hosts for evidence stability.
            if any(x in host for x in ("bilibili.com", "youtube.com", "v.qq.com")):
                score -= 120
            if any(x in host for x in ("cwl.gov.cn", "zhcw.com", "baike.baidu.com")):
                score += 80

            sec_evid[sec["id"]].append(
                {
                    "title": clean_text(title),
                    "url": url,
                    "host": host,
                    "evidence": ev,
                    "score": score,
                }
            )

    lines = []
    lines.append("# 双色球坊间技巧经验手册（证据段落版）")
    lines.append("")
    lines.append(f"- 生成时间: {now_ts()}")
    lines.append(f"- 正文可用样本: {len(ok_rows)}")
    lines.append("- 说明: 以下段落来自二次抓取正文提取，已按主题聚类，仍需结合风险意识独立判断。")
    lines.append("")

    section_stats = {}
    for sec in LOTTERY_SECTIONS:
        sid = sec["id"]
        pool = sec_evid[sid]

        # unique by url
        seen = set()
        host_seen = Counter()
        chosen = []
        for item in sorted(pool, key=lambda x: x["score"], reverse=True):
            u = item["url"]
            if u in seen:
                continue
            if host_seen[item["host"]] >= 3:
                continue
            seen.add(u)
            host_seen[item["host"]] += 1
            chosen.append(item)
            if len(chosen) >= 12:
                break

        lines.append(f"## {sec['title']}")
        lines.append("")
        if not chosen:
            lines.append("- 暂无足够正文证据，建议扩展来源或人工补采。")
            lines.append("")
            section_stats[sid] = 0
            continue

        lines.append("- 证据段落:")
        for i, item in enumerate(chosen, 1):
            lines.append(f"  - ({i}) {item['evidence']}")
            lines.append(f"    来源: [{item['title']}]({item['url']})")
        lines.append("")
        section_stats[sid] = len(chosen)

    out_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "ok_rows": len(ok_rows),
        "section_stats": section_stats,
    }


def verify_books(book_rows: List[dict], deep_map: Dict[str, dict], out_dir: Path) -> dict:
    items = []
    for row in book_rows:
        url = normalize_url(row.get("canonical_url", ""))
        if not url.startswith("http"):
            continue
        deep = deep_map.get(url, {})
        text_blob = "\n".join(
            [
                row.get("title", ""),
                row.get("snippet", ""),
                deep.get("title", ""),
                deep.get("snippet", ""),
                deep.get("text", "")[:3000],
            ]
        )
        isbns = extract_isbns(text_blob)
        pubs = extract_publishers(text_blob)
        authors = extract_authors(text_blob)
        if len(pubs) > 3:
            pubs = []
        if len(authors) > 5:
            authors = []

        host = host_of(url)
        title = clean_text(deep.get("title") or row.get("title") or url)
        tnorm = normalize_book_title(title)
        path = (urlparse(url).path or "").lower()
        is_catalog = any(k in path for k in ["/list", "/search", "/tag", "/doulists", "/review", "/video", "/topic"])

        has_isbn = bool(isbns)
        has_pub = bool(pubs)
        has_author = bool(authors)
        good_host = is_host_match(host, BOOK_GOOD_HOSTS)
        bad_host = is_host_match(host, BOOK_BAD_HOSTS)
        market_host = is_host_match(host, BOOK_MARKET_HOSTS)

        score = 0
        notes = []
        if has_isbn:
            score += 4
            notes.append("has_isbn")
        if has_pub:
            score += 3
            notes.append("has_publisher")
        if has_author:
            score += 1
            notes.append("has_author")

        text_l = text_blob.lower()
        if any(k.lower() in text_l for k in BOOK_SIGNAL):
            score += 2
            notes.append("book_signal")

        if good_host:
            score += 2
            notes.append("good_host")
        if bad_host:
            score -= 2
            notes.append("bad_host")
        if market_host:
            score -= 1
            notes.append("market_host")
        if is_catalog:
            score -= 1
            notes.append("catalog_page")

        if len(tnorm) < 6:
            score -= 1
            notes.append("weak_title")

        title_quality = 0
        if good_host:
            title_quality += 3
        if has_isbn:
            title_quality += 2
        if has_pub:
            title_quality += 1
        if 6 <= len(title) <= 60:
            title_quality += 1
        if market_host:
            title_quality -= 1
        if is_catalog:
            title_quality -= 1

        items.append(
            {
                "url": url,
                "host": host,
                "title": title,
                "title_norm": tnorm,
                "isbn_list": ";".join(isbns),
                "publisher_list": ";".join(pubs),
                "author_list": ";".join(authors),
                "score": score,
                "title_quality": title_quality,
                "has_isbn": has_isbn,
                "has_publisher": has_pub,
                "good_host": good_host,
                "market_host": market_host,
                "is_catalog": is_catalog,
                "notes": ";".join(notes),
                "query": row.get("query", ""),
            }
        )

    # Deduplicate by ISBN (preferred) then normalized title.
    groups: Dict[str, dict] = {}
    for it in items:
        isbn = it["isbn_list"].split(";")[0] if it["isbn_list"] else ""
        key = f"isbn:{isbn}" if isbn else f"title:{it['title_norm']}"
        if key not in groups:
            groups[key] = {
                "book_key": key,
                "title": it["title"],
                "title_norm": it["title_norm"],
                "isbn": isbn,
                "publishers": set(it["publisher_list"].split(";") if it["publisher_list"] else []),
                "authors": set(it["author_list"].split(";") if it["author_list"] else []),
                "source_urls": [it["url"]],
                "hosts": {it["host"]},
                "max_score": it["score"],
                "sum_score": it["score"],
                "count": 1,
                "notes": set(it["notes"].split(";") if it["notes"] else []),
                "best_title_quality": it["title_quality"],
                "good_host_any": it["good_host"],
                "market_host_any": it["market_host"],
                "has_isbn_any": it["has_isbn"],
                "has_publisher_any": it["has_publisher"],
            }
        else:
            g = groups[key]
            if it["title_quality"] > g["best_title_quality"]:
                g["title"] = it["title"]
                g["best_title_quality"] = it["title_quality"]
            if not g["isbn"] and isbn:
                g["isbn"] = isbn
            g["publishers"].update([x for x in it["publisher_list"].split(";") if x])
            g["authors"].update([x for x in it["author_list"].split(";") if x])
            g["source_urls"].append(it["url"])
            g["hosts"].add(it["host"])
            g["max_score"] = max(g["max_score"], it["score"])
            g["sum_score"] += it["score"]
            g["count"] += 1
            g["notes"].update([x for x in it["notes"].split(";") if x])
            g["good_host_any"] = g["good_host_any"] or it["good_host"]
            g["market_host_any"] = g["market_host_any"] or it["market_host"]
            g["has_isbn_any"] = g["has_isbn_any"] or it["has_isbn"]
            g["has_publisher_any"] = g["has_publisher_any"] or it["has_publisher"]

    ranked = []
    for _, g in groups.items():
        score = g["max_score"]
        if len(g["hosts"]) >= 2:
            score += 1
        if len(g["source_urls"]) >= 3:
            score += 1

        strong_cond = (
            g["has_isbn_any"]
            and g["has_publisher_any"]
            and (g["good_host_any"] or len(g["hosts"]) >= 2)
            and not (g["market_host_any"] and not g["good_host_any"])
        )
        medium_cond = (
            (g["has_isbn_any"] and (g["has_publisher_any"] or g["good_host_any"]))
            or (g["has_publisher_any"] and g["good_host_any"])
            or (score >= 7 and len(g["hosts"]) >= 2)
        )

        if strong_cond:
            conf = "强"
        elif medium_cond:
            conf = "中"
        else:
            conf = "弱"

        ranked.append(
            {
                "book_key": g["book_key"],
                "confidence": conf,
                "score": score,
                "title": g["title"],
                "isbn": g["isbn"],
                "publishers": ";".join(sorted([x for x in g["publishers"] if x])),
                "authors": ";".join(sorted([x for x in g["authors"] if x])),
                "source_count": len(g["source_urls"]),
                "hosts": ";".join(sorted(g["hosts"])),
                "evidence_urls": "\n".join(g["source_urls"][:8]),
                "notes": ";".join(sorted([x for x in g["notes"] if x])),
            }
        )

    ranked.sort(
        key=lambda x: (
            0 if x["confidence"] == "强" else 1 if x["confidence"] == "中" else 2,
            -x["score"],
            -x["source_count"],
            x["title"],
        )
    )

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = out_dir / f"book_verify_raw_{ts}.csv"
    ranked_path = out_dir / f"trusted_books_{ts}.csv"

    write_csv(
        raw_path,
        items,
        [
            "url",
            "host",
            "title",
            "title_norm",
            "isbn_list",
            "publisher_list",
            "author_list",
            "score",
            "title_quality",
            "has_isbn",
            "has_publisher",
            "good_host",
            "market_host",
            "is_catalog",
            "notes",
            "query",
        ],
    )

    write_csv(
        ranked_path,
        ranked,
        [
            "book_key",
            "confidence",
            "score",
            "title",
            "isbn",
            "publishers",
            "authors",
            "source_count",
            "hosts",
            "evidence_urls",
            "notes",
        ],
    )

    report_path = out_dir / f"trusted_books_report_{ts}.md"
    by_conf = {"强": [], "中": [], "弱": []}
    for r in ranked:
        by_conf[r["confidence"]].append(r)

    lines = []
    lines.append("# 双色球书籍线索核验报告（强/中/弱）")
    lines.append("")
    lines.append(f"- 生成时间: {now_ts()}")
    lines.append(f"- 输入线索: {len(book_rows)}")
    lines.append(f"- 去重后书目: {len(ranked)}")
    lines.append(f"- 强: {len(by_conf['强'])} / 中: {len(by_conf['中'])} / 弱: {len(by_conf['弱'])}")
    lines.append("")

    for conf in ["强", "中", "弱"]:
        lines.append(f"## {conf}可信")
        lines.append("")
        if not by_conf[conf]:
            lines.append("- 无")
            lines.append("")
            continue
        for i, r in enumerate(by_conf[conf], 1):
            title = r["title"] or r["book_key"]
            isbn = r["isbn"] or "-"
            pub = r["publishers"] or "-"
            lines.append(f"- {i}. {title}")
            lines.append(f"  - ISBN: {isbn}")
            lines.append(f"  - 出版社: {pub}")
            lines.append(f"  - 评分: {r['score']} | 来源数: {r['source_count']}")
            if r["evidence_urls"]:
                first = r["evidence_urls"].splitlines()[0]
                lines.append(f"  - 首条证据: {first}")
        lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")

    return {
        "book_raw_csv": str(raw_path),
        "trusted_books_csv": str(ranked_path),
        "trusted_books_report": str(report_path),
        "counts": {
            "input": len(book_rows),
            "dedup": len(ranked),
            "strong": len(by_conf["强"]),
            "medium": len(by_conf["中"]),
            "weak": len(by_conf["弱"]),
        },
    }


def write_coverage_report(summary: dict, out_path: Path) -> None:
    lines = []
    lines.append("# 正文二次抓取覆盖报告")
    lines.append("")
    lines.append(f"- 生成时间: {now_ts()}")
    lines.append(f"- 总任务: {summary.get('total', 0)}")
    st = summary.get("status", {})
    for k in sorted(st):
        lines.append(f"- {k}: {st[k]}")
    lines.append("")
    lines.append("## Top Hosts")
    lines.append("")
    for host, c in summary.get("top_hosts", [])[:40]:
        lines.append(f"- {host}: {c}")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Deep crawl and verify book leads.")
    ap.add_argument("--root", default="/Users/shine/lottery_research")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--timeout", type=int, default=15)
    ap.add_argument("--max-links", type=int, default=None, help="Debug limit for unique links.")
    ap.add_argument("--max-books", type=int, default=None, help="Debug limit for book leads.")
    ap.add_argument("--no-safari-fallback", action="store_true", help="Disable Safari fallback for blocked pages.")
    ap.add_argument("--safari-fallback-max", type=int, default=250, help="Max pages for Safari fallback.")
    ap.add_argument("--safari-wait", type=float, default=3.0, help="Seconds to wait after Safari navigation.")
    ap.add_argument("--reuse-deep-jsonl", default=None, help="Reuse an existing deep_pages_*.jsonl and skip crawling.")
    args = ap.parse_args()

    root = Path(args.root)
    data_dir = root / "data"
    reports_dir = root / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    unique_csv, book_csv, _ = choose_latest_paths(root)
    unique_rows = read_csv(unique_csv)
    book_rows = read_csv(book_csv)
    if args.max_links:
        unique_rows = unique_rows[: args.max_links]
    if args.max_books:
        book_rows = book_rows[: args.max_books]

    print(f"input unique links: {len(unique_rows)}")
    print(f"input book leads: {len(book_rows)}")

    if args.reuse_deep_jsonl:
        reuse_path = Path(args.reuse_deep_jsonl)
        results = read_jsonl(reuse_path)
        status_counter = Counter(r.get("status", "unknown") for r in results)
        host_counter = Counter(host_of(r.get("url", "")) for r in results)
        summary = {
            "deep_jsonl": str(reuse_path),
            "deep_csv": "",
            "total": len(results),
            "status": dict(status_counter),
            "safari_fallback_used": 0,
            "safari_fallback_recovered": 0,
            "top_hosts": host_counter.most_common(30),
        }
    else:
        results, summary = crawl_all(
            unique_rows,
            data_dir,
            workers=args.workers,
            timeout=args.timeout,
            safari_fallback=not args.no_safari_fallback,
            safari_fallback_max=args.safari_fallback_max,
            safari_wait=args.safari_wait,
        )
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    coverage_report = reports_dir / f"deep_crawl_coverage_{ts}.md"
    write_coverage_report(summary, coverage_report)

    handbook_path = reports_dir / f"experience_handbook_evidence_{ts}.md"
    handbook_stats = build_evidence_handbook(results, handbook_path)

    deep_map = {r["url"]: r for r in results}
    book_summary = verify_books(book_rows, deep_map, data_dir)

    final = {
        "timestamp": ts,
        "source_unique_csv": str(unique_csv),
        "source_book_csv": str(book_csv),
        "deep_crawl": summary,
        "coverage_report": str(coverage_report),
        "evidence_handbook": str(handbook_path),
        "handbook_stats": handbook_stats,
        "book_verify": book_summary,
    }

    out_json = root / "deep_enrich_latest.json"
    out_json.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== DEEP ENRICH DONE ===")
    print(json.dumps(final, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
