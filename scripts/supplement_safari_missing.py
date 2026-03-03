#!/usr/bin/env python3
"""Supplement deep crawl missing/thin pages via Safari session."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import List
from urllib.parse import urlparse, urlunparse

SAFARI_SCRIPTS = "/Users/shine/Downloads/skills/webview-assistant/scripts"
if SAFARI_SCRIPTS not in sys.path:
    sys.path.append(SAFARI_SCRIPTS)

from safari_ops import SafariOps  # type: ignore

REQUIRE_WWW_HOSTS = {
    "zhcw.com",
    "cpzj.com",
    "cwl.gov.cn",
    "00038.cn",
    "yclottery.com",
}


def clean_text(text: str) -> str:
    t = (text or "").replace("\u00a0", " ")
    t = re.sub(r"[\t\r\f\v]+", "\n", t)
    t = re.sub(r"\n{2,}", "\n", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def split_lines(text: str) -> List[str]:
    out = []
    seen = set()
    for line in re.split(r"\n+", text or ""):
        s = clean_text(line)
        if len(s) < 10:
            continue
        k = s[:120]
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def meaningful(text: str, title: str) -> bool:
    t = clean_text(text)
    if len(t) < 180:
        return False
    bad_tokens = [
        "访问受限",
        "访问异常",
        "403",
        "验证码",
        "请登录",
        "登录后查看",
        "网络错误",
        "安全验证",
        "页面不存在",
    ]
    low = (title + " " + t[:400]).lower()
    if any(tok.lower() in low for tok in bad_tokens):
        return False
    return True


def host_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


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
        return u
    return u


def main() -> None:
    ap = argparse.ArgumentParser(description="Safari supplement for missing deep pages")
    ap.add_argument("--input-jsonl", required=True)
    ap.add_argument("--wait", type=float, default=2.8)
    ap.add_argument("--max", type=int, default=None)
    ap.add_argument(
        "--hosts",
        default="",
        help="Comma-separated host filters (e.g. zhcw.com,cwl.gov.cn). Empty means all missing.",
    )
    ap.add_argument(
        "--statuses",
        default="failed,http_error,thin",
        help="Comma-separated statuses to target.",
    )
    args = ap.parse_args()

    input_path = Path(args.input_jsonl)
    rows = [json.loads(line) for line in input_path.open(encoding="utf-8") if line.strip()]

    target_status = {s.strip() for s in args.statuses.split(",") if s.strip()}
    host_filters = [h.strip().lower() for h in args.hosts.split(",") if h.strip()]

    def host_match(url: str) -> bool:
        h = host_of(url)
        if not host_filters:
            return True
        return any(h == d or h.endswith("." + d) for d in host_filters)

    targets = [r for r in rows if r.get("status") in target_status and host_match(r.get("url", ""))]
    if args.max:
        targets = targets[: args.max]

    print(f"input rows={len(rows)} targets={len(targets)}")

    ops = SafariOps()
    recovered = 0
    attempted = 0
    by_host = Counter()

    row_map = {r.get("url"): r for r in rows}

    for i, item in enumerate(targets, 1):
        url = item.get("url", "")
        if not url.startswith("http"):
            continue
        url = normalize_url(url)
        attempted += 1
        h = host_of(url)
        by_host[h] += 1

        # Domain-specific wait uplift for heavy pages.
        wait_s = args.wait
        if any(x in h for x in ["zhihu.com", "baidu.com", "weixin.qq.com", "mp.weixin.qq.com"]):
            wait_s += 0.8

        try:
            ops.navigate_to(url)
            time.sleep(wait_s)
            cur = ops.get_current_url() or url
            title = ops.execute_js("document.title") or ""
            page_text = ops.get_page_text() or ""
            text = clean_text(page_text)
            lines = split_lines(text)
            if lines:
                text = "\n".join(lines[:220])

            if meaningful(text, title):
                row = row_map.get(url, item)
                row["status"] = "ok"
                row["http_status"] = row.get("http_status") or "safari"
                row["final_url"] = cur
                row["title"] = clean_text(title)
                row["snippet"] = clean_text(text[:280])
                row["text"] = text
                row["text_len"] = len(text)
                row["error"] = ""
                row["fetched_at"] = dt.datetime.now().isoformat(timespec="seconds")
                recovered += 1
            else:
                # record small diagnostic to help next retry
                row = row_map.get(url, item)
                prev = row.get("error", "")
                row["error"] = (prev + " | safari_not_meaningful").strip(" |")[:240]
        except Exception as exc:
            row = row_map.get(url, item)
            prev = row.get("error", "")
            row["error"] = (prev + f" | safari_err:{exc}").strip(" |")[:240]

        if i % 20 == 0 or i == len(targets):
            print(f"progress {i}/{len(targets)} recovered={recovered}")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_jsonl = input_path.parent / f"deep_pages_supp_{ts}.jsonl"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    out_csv = input_path.parent / f"deep_pages_supp_{ts}.csv"
    fields = [
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
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

    status_counter = Counter(r.get("status", "") for r in rows)
    summary = {
        "input": str(input_path),
        "output_jsonl": str(out_jsonl),
        "output_csv": str(out_csv),
        "targets": len(targets),
        "attempted": attempted,
        "recovered": recovered,
        "status": dict(status_counter),
        "target_hosts": by_host.most_common(30),
    }
    out_summary = input_path.parent / f"deep_pages_supp_summary_{ts}.json"
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== SUPPLEMENT DONE ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
