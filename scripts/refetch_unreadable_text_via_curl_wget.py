#!/usr/bin/env python3
"""Analyze text readability row-by-row and refetch unreadable pages via curl/wget."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

NAV_TOKENS = [
    "首页",
    "登录",
    "注册",
    "下载",
    "app",
    "客户端",
    "扫码",
    "微信",
    "微博",
    "导航",
    "搜索",
    "关注",
    "上一页",
    "下一页",
    "返回顶部",
    "客服",
    "版权",
    "免责声明",
    "广告",
    "彩票开奖",
    "走势图",
]

SENT_PUNC = "。！？；!?…"
MOJIBAKE_RE = re.compile(
    r"[ÃÂâäåæçèéêëìíîïðñòóôõöøùúûüýþÿÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞß]"
)
CHARSET_RE = re.compile(
    r"charset\s*=\s*['\"]?([A-Za-z0-9_\-]+)", flags=re.IGNORECASE
)
URL_RE = re.compile(r"^https?://", re.IGNORECASE)
TRANSFORMS = [
    ("latin1", "utf-8"),
    ("cp1252", "utf-8"),
    ("latin1", "gbk"),
    ("latin1", "gb18030"),
    ("cp1252", "gbk"),
    ("cp1252", "gb18030"),
]


def normalize_text(text: str) -> str:
    t = text or ""
    t = t.replace("\u00a0", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def core_text(text: str) -> str:
    return "".join(ch for ch in (text or "") if not ch.isspace())


def readability_metrics(text: str) -> dict[str, Any]:
    s = normalize_text(text)
    c = core_text(s)
    if not c:
        return {
            "bad": True,
            "score": -10.0,
            "reason": "empty",
            "len": 0,
            "punc": 0,
            "nav_hits": 0,
            "digit_ratio": 0.0,
            "cjk_ratio": 0.0,
            "long_lines": 0,
        }

    cjk_n = sum(1 for ch in c if "\u4e00" <= ch <= "\u9fff")
    digit_n = sum(1 for ch in c if ch.isdigit())
    cjk_ratio = cjk_n / len(c)
    digit_ratio = digit_n / len(c)
    punc = sum(s.count(x) for x in SENT_PUNC)
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", s) if ln.strip()]
    long_lines = sum(1 for ln in lines if len(ln) >= 40)
    nav_hits = sum(s.lower().count(tk) for tk in NAV_TOKENS)
    moj_hits = len(MOJIBAKE_RE.findall(s))

    reasons: list[str] = []
    if len(c) < 140:
        reasons.append("too_short")
    if punc < 4 and long_lines < 2:
        reasons.append("low_sentence_structure")
    if nav_hits >= 16:
        reasons.append("menu_heavy")
    if digit_ratio > 0.42 and punc < 10:
        reasons.append("digit_heavy")
    if cjk_ratio < 0.05 and punc < 3:
        reasons.append("non_article")
    if moj_hits > 4:
        reasons.append("mojibake")

    score = 0.0
    score += min(len(c) / 600.0, 5.0)
    score += min(punc, 40) * 0.20
    score += min(long_lines, 40) * 0.25
    score += cjk_ratio * 2.0
    score -= nav_hits * 0.18
    score -= max(0.0, digit_ratio - 0.35) * 8.0
    score -= moj_hits * 0.30
    score -= len(reasons) * 0.8

    return {
        "bad": len(reasons) > 0,
        "score": round(score, 4),
        "reason": ",".join(reasons) if reasons else "ok",
        "len": len(c),
        "punc": punc,
        "nav_hits": nav_hits,
        "digit_ratio": round(digit_ratio, 4),
        "cjk_ratio": round(cjk_ratio, 4),
        "long_lines": long_lines,
    }


def text_quality_score(text: str) -> float:
    s = normalize_text(text)
    c = core_text(s)
    if not c:
        return -100.0
    cjk = sum(1 for ch in c if "\u4e00" <= ch <= "\u9fff")
    moj = len(MOJIBAKE_RE.findall(s))
    punc = sum(s.count(x) for x in SENT_PUNC)
    digit_ratio = sum(ch.isdigit() for ch in c) / len(c)
    return cjk * 2.0 + punc * 1.3 - moj * 10.0 - max(0.0, digit_ratio - 0.55) * 200.0


def recover_mojibake(text: str) -> str:
    s = text or ""
    if not s:
        return s
    if len(MOJIBAKE_RE.findall(s)) <= 3:
        return s

    best = s
    best_score = text_quality_score(s)
    cur = s
    for _ in range(2):
        round_best = cur
        round_score = text_quality_score(cur)
        for src, dst in TRANSFORMS:
            for enc_err, dec_err in [("strict", "strict"), ("ignore", "ignore"), ("strict", "ignore")]:
                try:
                    cand = cur.encode(src, errors=enc_err).decode(dst, errors=dec_err)
                except Exception:
                    continue
                if not cand:
                    continue
                # Avoid destructive shrinks.
                if len(core_text(cand)) < len(core_text(cur)) * 0.35:
                    continue
                sc = text_quality_score(cand)
                if sc > round_score + 1.2:
                    round_best = cand
                    round_score = sc
        if round_best == cur:
            break
        cur = round_best
        if round_score > best_score:
            best = round_best
            best_score = round_score
    return best


def decode_bytes(raw: bytes) -> tuple[str, str]:
    head = raw[:6000]
    encs: list[str] = []
    m = CHARSET_RE.search(head.decode("latin1", errors="ignore"))
    if m:
        encs.append(m.group(1).lower())
    encs.extend(["utf-8", "gb18030", "gbk", "big5", "latin1"])

    seen = set()
    uniq_encs = []
    for e in encs:
        e = e.strip().lower()
        if not e or e in seen:
            continue
        seen.add(e)
        uniq_encs.append(e)

    best_txt = ""
    best_enc = "utf-8"
    best_score = -10**9

    for enc in uniq_encs:
        try:
            txt = raw.decode(enc, errors="ignore")
        except Exception:
            continue
        c = core_text(txt)
        if not c:
            continue
        cjk = sum(1 for ch in c if "\u4e00" <= ch <= "\u9fff")
        moj = len(MOJIBAKE_RE.findall(txt))
        html_hint = 1 if "<html" in txt.lower() else 0
        score = cjk * 2.0 - moj * 12.0 + len(c) * 0.02 + html_hint * 30
        if score > best_score:
            best_score = score
            best_txt = txt
            best_enc = enc
    return best_txt, best_enc


def looks_binary_payload(raw: bytes) -> bool:
    if not raw:
        return True
    sample = raw[:12000]
    # Text payload should rarely contain many control bytes.
    ctrl = sum(1 for b in sample if (b < 9) or (13 < b < 32))
    return (ctrl / max(1, len(sample))) > 0.02


def clean_lines(lines: list[str]) -> list[str]:
    out: list[str] = []
    prev = ""
    junk = {"展开", "收起", "更多", "加载中", "点击查看", "相关推荐", "版权声明"}
    for ln in lines:
        s = normalize_text(ln)
        if not s:
            continue
        if s in junk:
            continue
        if re.fullmatch(r"[0-9\-\./: ]{6,}", s):
            continue
        low = s.lower()
        nav_line_hits = sum(1 for tk in NAV_TOKENS if tk in low)
        # Drop nav/menu link clusters.
        if nav_line_hits >= 2 and len(s) <= 140:
            continue
        if nav_line_hits >= 1 and len(s) <= 80 and "http" not in s:
            continue
        token_parts = [x for x in re.split(r"[|/>\u3000 ]+", s) if x]
        if len(token_parts) >= 10 and sum(s.count(x) for x in SENT_PUNC) == 0:
            continue
        if s == prev:
            continue
        prev = s
        out.append(s)
    return out


def has_text_noise(text: str) -> bool:
    s = text or ""
    if not s:
        return True
    c = core_text(s)
    if not c:
        return True
    ctrl = sum(1 for ch in s if ord(ch) < 32 and ch not in "\n\r\t")
    if ctrl / max(1, len(s)) > 0.01:
        return True
    weird = sum(1 for ch in s if ch == "�")
    if weird > 12:
        return True
    return False


def extract_best_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(
        [
            "script",
            "style",
            "noscript",
            "svg",
            "form",
            "button",
            "iframe",
            "canvas",
            "footer",
            "header",
            "nav",
            "aside",
        ]
    ):
        tag.decompose()

    selectors = [
        "article",
        "main",
        "#article",
        "#content",
        ".article",
        ".article-content",
        ".post-content",
        ".entry-content",
        ".news-content",
        ".detail-content",
        "#js_content",
        ".rich_media_content",
    ]

    candidates: list[tuple[str, str]] = []

    # Prefer explicit meta descriptions for portal/video pages.
    for key, value in [
        ("name", "description"),
        ("property", "og:description"),
        ("name", "twitter:description"),
    ]:
        tag = soup.find("meta", attrs={key: value})
        if tag and tag.get("content"):
            candidates.append(("meta", normalize_text(str(tag.get("content")))))

    for sel in selectors:
        for node in soup.select(sel):
            txt = normalize_text(node.get_text("\n", strip=True))
            if txt:
                candidates.append(("selector", txt))

    # fallback: evaluate div/section with content-like id/class
    if not candidates:
        key_re = re.compile(
            r"(article|content|detail|post|entry|text|news|正文|内容|资讯|main)",
            flags=re.IGNORECASE,
        )
        for node in soup.find_all(["div", "section"]):
            id_cls = " ".join(
                [str(node.get("id") or ""), " ".join(node.get("class") or [])]
            )
            if not key_re.search(id_cls):
                continue
            txt = normalize_text(node.get_text("\n", strip=True))
            if len(core_text(txt)) >= 160:
                candidates.append(("keyed_div", txt))

    body_txt = normalize_text((soup.body or soup).get_text("\n", strip=True))
    if body_txt:
        candidates.append(("body", body_txt))

    best = ""
    best_score = -10**9
    for kind, txt in candidates:
        lines = clean_lines([x for x in txt.split("\n") if x.strip()])
        # keep meaningful lines only
        lines = [x for x in lines if len(x) >= 10 or re.search(r"[。！？!?]", x)]
        if len(lines) > 260:
            lines = lines[:260]
        t = normalize_text("\n".join(lines))
        m = readability_metrics(t)
        score = m["score"]
        if kind == "meta":
            # Meta descriptions are often the most coherent summary on media pages.
            score += 1.6
            if m["punc"] >= 1:
                score += 0.8
        if kind == "body" and m["nav_hits"] >= 12:
            score -= 2.0
        if score > best_score:
            best_score = score
            best = t
    return best


def fetch_html_bytes(url: str, timeout_sec: int = 20) -> tuple[bytes | None, str, str]:
    if not url or not URL_RE.match(url):
        return None, "none", "invalid_url"

    curl_cmd = [
        "curl",
        "-L",
        "-sS",
        "--max-time",
        str(timeout_sec),
        "--connect-timeout",
        "8",
        "-A",
        USER_AGENT,
        "-H",
        "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
        url,
    ]
    try:
        r = subprocess.run(
            curl_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout_sec + 5,
        )
        if r.returncode == 0 and len(r.stdout) >= 500:
            return r.stdout, "curl", ""
        curl_err = r.stderr.decode("utf-8", errors="ignore")[:200]
    except Exception as e:
        curl_err = f"curl_exception:{e}"

    wget_err = "wget_not_installed"
    if shutil.which("wget"):
        wget_cmd = [
            "wget",
            "-q",
            "-O",
            "-",
            "--timeout=20",
            "--tries=1",
            "--max-redirect=5",
            "--user-agent",
            USER_AGENT,
            url,
        ]
        try:
            r = subprocess.run(
                wget_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=timeout_sec + 5,
            )
            if r.returncode == 0 and len(r.stdout) >= 500:
                return r.stdout, "wget", ""
            wget_err = r.stderr.decode("utf-8", errors="ignore")[:200]
        except Exception as e:
            wget_err = f"wget_exception:{e}"

    return None, "none", f"{curl_err} | {wget_err}"


def should_accept(old_m: dict[str, Any], new_m: dict[str, Any]) -> bool:
    if new_m["len"] < 180:
        return False
    if old_m["bad"] and not new_m["bad"]:
        return True
    if new_m["score"] >= old_m["score"] + 2.0:
        return True
    if old_m["bad"] and new_m["score"] >= old_m["score"] + 1.0:
        return True
    # if both are bad, still accept large quality uplift
    old_reason_n = len([x for x in str(old_m["reason"]).split(",") if x and x != "ok"])
    new_reason_n = len([x for x in str(new_m["reason"]).split(",") if x and x != "ok"])
    if new_reason_n + 1 <= old_reason_n and new_m["score"] > old_m["score"]:
        return True
    return False


def process_one(idx: int, row: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any]]:
    url = str(row.get("final_url") or row.get("url") or "")
    old_text = str(row.get("text", "") or "")
    old_m = readability_metrics(old_text)

    detail: dict[str, Any] = {
        "idx": idx,
        "url": row.get("url", ""),
        "final_url": row.get("final_url", ""),
        "old_bad": "yes" if old_m["bad"] else "no",
        "old_score": old_m["score"],
        "old_reason": old_m["reason"],
        "old_len": old_m["len"],
        "fetch_method": "",
        "decode_enc": "",
        "new_score": "",
        "new_reason": "",
        "new_len": "",
        "updated": "no",
        "note": "",
    }

    if not old_m["bad"]:
        detail["note"] = "already_readable"
        return idx, row, detail

    raw, method, err = fetch_html_bytes(url)
    detail["fetch_method"] = method
    if raw is None:
        detail["note"] = f"fetch_failed:{err[:220]}"
        return idx, row, detail

    # Skip obvious non-html payload.
    if raw.startswith(b"%PDF"):
        detail["note"] = "non_html_pdf"
        return idx, row, detail
    if looks_binary_payload(raw):
        detail["note"] = "binary_payload"
        return idx, row, detail
    if raw[:300].strip().startswith(b"{") and b"<html" not in raw[:3000].lower():
        detail["note"] = "non_html_json"
        return idx, row, detail

    html, enc = decode_bytes(raw)
    detail["decode_enc"] = enc
    if not html:
        detail["note"] = "decode_empty"
        return idx, row, detail

    new_text = extract_best_text(html)
    if not new_text and html:
        # For plaintext-ish responses, keep decoded content directly.
        new_text = normalize_text(html)
    new_text = recover_mojibake(new_text)
    if has_text_noise(new_text):
        detail["note"] = "noisy_text_after_parse"
        return idx, row, detail
    new_m = readability_metrics(new_text)
    detail["new_score"] = new_m["score"]
    detail["new_reason"] = new_m["reason"]
    detail["new_len"] = new_m["len"]

    if should_accept(old_m, new_m):
        row2 = dict(row)
        row2["text"] = new_text
        row2["text_len"] = len(new_text)
        detail["updated"] = "yes"
        detail["note"] = "reparsed_from_source"
        return idx, row2, detail

    detail["note"] = "not_improved_enough"
    return idx, row, detail


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Analyze readability and refetch unreadable texts via curl/wget."
    )
    ap.add_argument("--input-jsonl", required=True)
    ap.add_argument("--out-dir", default="/Users/shine/lottery_research/data")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    inp = Path(args.input_jsonl)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = [json.loads(l) for l in inp.open(encoding="utf-8") if l.strip()]

    # Mark candidates first.
    cands = []
    for i, r in enumerate(rows):
        if readability_metrics(str(r.get("text", ""))).get("bad"):
            cands.append(i)
    if args.limit and args.limit > 0:
        cands = cands[: args.limit]

    updated_rows = list(rows)
    details: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = [ex.submit(process_one, i, rows[i]) for i in cands]
        done = 0
        total = len(futs)
        for fut in as_completed(futs):
            i, new_row, detail = fut.result()
            updated_rows[i] = new_row
            details.append(detail)
            done += 1
            if done % 25 == 0 or done == total:
                print(f"progress {done}/{total}")

    # Recheck final readability.
    before_bad = sum(1 for r in rows if readability_metrics(str(r.get("text", "")))["bad"])
    after_bad = sum(
        1 for r in updated_rows if readability_metrics(str(r.get("text", "")))["bad"]
    )
    updated_cnt = sum(1 for d in details if d.get("updated") == "yes")
    fetch_fail_cnt = sum(1 for d in details if str(d.get("note", "")).startswith("fetch_failed"))

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_jsonl = out_dir / f"正文库_可读性修复_{ts}.jsonl"
    out_csv = out_dir / f"正文库_可读性修复_{ts}.csv"
    detail_jsonl = out_dir / f"正文库_可读性修复明细_{ts}.jsonl"
    detail_csv = out_dir / f"正文库_可读性修复明细_{ts}.csv"
    summary_path = out_dir / f"正文库_可读性修复汇总_{ts}.json"

    with out_jsonl.open("w", encoding="utf-8") as f:
        for r in updated_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    with detail_jsonl.open("w", encoding="utf-8") as f:
        for d in details:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    write_csv(
        out_csv,
        updated_rows,
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
            "title_is_garbled",
            "text",
        ],
    )

    write_csv(
        detail_csv,
        sorted(details, key=lambda x: x.get("idx", 0)),
        [
            "idx",
            "url",
            "final_url",
            "old_bad",
            "old_score",
            "old_reason",
            "old_len",
            "fetch_method",
            "decode_enc",
            "new_score",
            "new_reason",
            "new_len",
            "updated",
            "note",
        ],
    )

    summary = {
        "input": str(inp),
        "input_total": len(rows),
        "candidate_total": len(cands),
        "updated_total": updated_cnt,
        "fetch_failed_total": fetch_fail_cnt,
        "before_unreadable_total": before_bad,
        "after_unreadable_total": after_bad,
        "outputs": {
            "updated_jsonl": str(out_jsonl),
            "updated_csv": str(out_csv),
            "detail_jsonl": str(detail_jsonl),
            "detail_csv": str(detail_csv),
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
