#!/usr/bin/env python3
"""Split unfetched/risk/unrelated rows and produce cleaned corpus."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


LOTTERY_KEYWORDS = [
    "双色球", "福彩", "彩票", "选号", "红球", "蓝球", "胆拖", "杀号", "和值", "连号", "中奖",
    "ssq", "lottery", "double chromosphere",
]

RISK_LOGIN_TOKENS = [
    "请进行安全验证",
    "安全验证",
    "验证异常",
    "请登录",
    "登录后",
    "登录/注册",
    "欢迎登录",
    "访问受限",
    "访问异常",
    "验证码",
    "forbidden",
    "deny",
    "无权限",
]

NOT_FOUND_TOKENS = [
    "404",
    "页面不存在",
    "未找到页面",
    "not found",
]

UNRELATED_TOKENS = [
    "高清1080p在线观看平台_腾讯视频",
    "优惠商品可能已下架",
    "app store",
    "提示信息_360百科",
    "您的浏览器版本过低",
    "你似乎来到了没有知识存在的荒原",
    "京东-欢迎登录",
    "哔哩哔哩 (゜-゜)つロ 干杯~-bilibili",
    "bilibili (゜-゜)つロ 干杯",
]

UNRELATED_HOSTS = [
    "murata.com",
]

STOCK_DIVERSION_TOKENS = [
    "助力炒股",
    "应用于股票交易",
    "股票交易",
    "精准选股",
    "选股高手",
]

LOTTERY_HOST_HINTS = [
    "zhcw.com",
    "cwl.gov.cn",
    "500.com",
    "cpzj.com",
    "17500.cn",
    "00038.cn",
    "dgflcp.com",
    "fcsnsc.cn",
    "sports.sohu.com",
    "caipiao.sohu.com",
    "sports.sina.com.cn",
    "lotto.sina.cn",
    "zx.500.com",
    "baike.baidu.com",
    "zhihu.com",
    "zhuanlan.zhihu.com",
    "tieba.baidu.com",
    "k.sina.com.cn",
    "finance.sina.com.cn",
    "bj.sina.com.cn",
    "heyuanxw.com",
    "bbss.17500.cn",
    "mzt.hunan.gov.cn",
]


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def host_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main() -> None:
    ap = argparse.ArgumentParser(description="Clean deep corpus and split unfetched table.")
    ap.add_argument("--input-jsonl", required=True)
    ap.add_argument("--out-dir", default="/Users/shine/lottery_research/data")
    args = ap.parse_args()

    inp = Path(args.input_jsonl)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = [json.loads(l) for l in inp.open(encoding="utf-8") if l.strip()]

    unfetched: list[dict] = []
    cleaned: list[dict] = []

    initial_unfetched_status = {"failed", "http_error", "thin"}
    unresolved_status = {"failed", "http_error", "thin", "non_html"}

    for r in rows:
        status = str(r.get("status", ""))
        http_status = str(r.get("http_status", ""))
        url = r.get("url", "")
        final_url = r.get("final_url", "")
        h = host_of(url)
        text = " ".join([str(r.get("title", "")), str(r.get("snippet", "")), str(r.get("error", ""))]).lower()

        reasons: list[str] = []

        if status in unresolved_status:
            reasons.append(f"抓取状态:{status}")

        if http_status in {"404", "521"} or any(t in text for t in [x.lower() for x in NOT_FOUND_TOKENS]):
            reasons.append("404/页面失效")

        if http_status in {"403", "451"} or any(t in text for t in [x.lower() for x in RISK_LOGIN_TOKENS]):
            reasons.append("风控或需登录")

        if "mp.weixin.qq.com" in h:
            reasons.append("时效链接/公众号限制")

        # obvious unrelated pages
        if any(t in text for t in [x.lower() for x in UNRELATED_TOKENS]):
            reasons.append("不相关或低价值页面")

        combined = " ".join([
            str(r.get("title", "")),
            str(r.get("snippet", "")),
            str(r.get("text", ""))[:2000],
            str(r.get("query", "")),
            str(r.get("title_seed", "")),
        ]).lower()
        has_kw = any(k.lower() in combined for k in LOTTERY_KEYWORDS)
        host_hint = any(h == d or h.endswith("." + d) for d in LOTTERY_HOST_HINTS)

        if h in UNRELATED_HOSTS or any(h.endswith("." + d) for d in UNRELATED_HOSTS):
            reasons.append("不相关或低价值页面")

        if h == "55188.com" and any(t in combined for t in STOCK_DIVERSION_TOKENS):
            reasons.append("不相关或低价值页面")

        # keep rule for cleaned corpus
        keep = True
        if reasons:
            keep = False
        elif status != "ok":
            keep = False
        elif int(r.get("text_len", 0) or 0) < 180:
            keep = False
            reasons.append("正文过短")
        elif not (has_kw or host_hint):
            keep = False
            reasons.append("不相关内容")

        if keep:
            cleaned.append(r)
        else:
            u = {
                "url": url,
                "final_url": final_url,
                "host": h,
                "status": status,
                "http_status": http_status,
                "title": norm(str(r.get("title", "")))[:300],
                "snippet": norm(str(r.get("snippet", "")))[:500],
                "error": norm(str(r.get("error", "")))[:300],
                "query": r.get("query", ""),
                "engines": r.get("engines", ""),
                "best_rank": r.get("best_rank", ""),
                "url_hash": r.get("url_hash", ""),
                "category": r.get("category", ""),
                "title_seed": r.get("title_seed", ""),
                "text_len": r.get("text_len", ""),
                "fetched_at": r.get("fetched_at", ""),
                "reason": "；".join(sorted(set(reasons))) if reasons else "不相关内容",
                "is_initial_unfetched": "yes" if status in initial_unfetched_status else "no",
            }
            unfetched.append(u)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    # outputs
    unf_jsonl = out_dir / f"未抓取_{ts}.jsonl"
    unf_csv = out_dir / f"未抓取_{ts}.csv"
    clean_jsonl = out_dir / f"正文库_清洗_{ts}.jsonl"
    clean_csv = out_dir / f"正文库_清洗_{ts}.csv"
    summary_path = out_dir / f"正文库清洗汇总_{ts}.json"

    with unf_jsonl.open("w", encoding="utf-8") as f:
        for r in unfetched:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    with clean_jsonl.open("w", encoding="utf-8") as f:
        for r in cleaned:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    write_csv(
        unf_csv,
        unfetched,
        [
            "url",
            "final_url",
            "host",
            "status",
            "http_status",
            "title",
            "snippet",
            "error",
            "query",
            "engines",
            "best_rank",
            "url_hash",
            "category",
            "title_seed",
            "text_len",
            "fetched_at",
            "reason",
            "is_initial_unfetched",
        ],
    )

    write_csv(
        clean_csv,
        cleaned,
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

    reason_counter = Counter()
    for r in unfetched:
        for x in str(r.get("reason", "")).split("；"):
            if x:
                reason_counter[x] += 1

    summary = {
        "input": str(inp),
        "input_total": len(rows),
        "cleaned_total": len(cleaned),
        "unfetched_total": len(unfetched),
        "initial_unfetched_count_in_unfetched": sum(1 for r in unfetched if r.get("is_initial_unfetched") == "yes"),
        "reason_counts": dict(reason_counter),
        "outputs": {
            "unfetched_jsonl": str(unf_jsonl),
            "unfetched_csv": str(unf_csv),
            "cleaned_jsonl": str(clean_jsonl),
            "cleaned_csv": str(clean_csv),
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
