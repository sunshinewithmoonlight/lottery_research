#!/usr/bin/env python3
"""Prune rows whose snippet is unreadable gibberish and not recoverable by common recoding."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from pathlib import Path


ARG1_TOKEN_RE = re.compile(r"var\s+arg1\s*=\s*['\"]", re.IGNORECASE)
B64_RE = re.compile(r"[A-Za-z0-9+/]{80,}={0,2}")
CJK_RE = re.compile(r"[\u4e00-\u9fff]")
MOJIBAKE_RE = re.compile(r"[ÃÂâäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ]")
COMMON_CJK = set(
    "的一是在不了有和人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动同工也能下过子说产种面而方后多定行学法所民得经十三之进着等部度家电力里如水化高自二理起小物现实加量都两体制机当使点从业本去把性好应开它合还因由其些然前外天政四日那社义事平形相全表间样与关各重新线内数正心反你明看原又么利比或但质气第向道命此变条只没结解问意建月公无系军很情者最立代想已通并提直题党程展五果料象员革位入常文总次品式活设及管特件长求老"
)
RECOVER_HINTS = [
    "双色球",
    "彩票",
    "福彩",
    "乐彩网",
    "开奖",
    "论坛",
    "选号",
]


def cjk_ratio(text: str) -> float:
    s = text or ""
    core = "".join(ch for ch in s if not ch.isspace())
    if not core:
        return 0.0
    cjk = len(CJK_RE.findall(core))
    return cjk / len(core)


def cjk_stats(text: str) -> tuple[int, int]:
    zh = CJK_RE.findall(text or "")
    if not zh:
        return 0, 0
    common = sum(1 for ch in zh if ch in COMMON_CJK)
    return len(zh), common


def looks_unreadable_candidate(snippet: str) -> bool:
    s = (snippet or "").strip()
    if not s:
        return False

    # Most real Chinese snippets should have visible CJK characters.
    if cjk_ratio(s) > 0.15:
        return False

    if ARG1_TOKEN_RE.search(s):
        return True
    if B64_RE.search(s):
        return True

    if MOJIBAKE_RE.search(s):
        core = "".join(ch for ch in s if not ch.isspace())
        moj = len(MOJIBAKE_RE.findall(core))
        if core and (moj / len(core)) > 0.06:
            return True

    # Extremely symbol-heavy strings are usually anti-bot script fragments.
    core = "".join(ch for ch in s if not ch.isspace())
    if not core:
        return False
    sym = sum(1 for ch in core if not ch.isalnum() and ch not in "，。！？；：,.!?;:()[]{}<>-_/\\'\"")
    return len(core) >= 120 and (sym / len(core)) > 0.25


def try_recover(snippet: str) -> str | None:
    s = snippet or ""
    pairs = [
        ("latin1", "utf-8"),
        ("cp1252", "utf-8"),
        ("latin1", "gb18030"),
        ("cp1252", "gb18030"),
        ("gb18030", "utf-8"),
        ("gbk", "utf-8"),
    ]

    def score(txt: str) -> float:
        core = "".join(ch for ch in txt if not ch.isspace())
        if not core:
            return -1.0
        cjk, common = cjk_stats(core)
        moj = len(MOJIBAKE_RE.findall(core))
        repl = txt.count("�")
        sym = sum(1 for ch in core if not ch.isalnum() and ch not in "，。！？；：,.!?;:()[]{}<>-_/\\'\"")
        pua = sum(1 for ch in core if 0xE000 <= ord(ch) <= 0xF8FF)
        return common * 4.0 + cjk * 0.4 - moj * 1.3 - repl * 3.0 - sym * 0.2 - pua * 2.5

    best = s
    best_score = score(s)

    def try_add(candidate: str) -> None:
        nonlocal best, best_score
        sc = score(candidate)
        if sc > best_score:
            best = candidate
            best_score = sc

    current = s
    for _ in range(3):
        round_best = current
        round_score = score(current)
        for src, dst in pairs:
            for enc_err, dec_err in [("strict", "strict"), ("ignore", "ignore"), ("ignore", "replace")]:
                try:
                    cand = current.encode(src, errors=enc_err).decode(dst, errors=dec_err)
                except Exception:
                    continue
                sc = score(cand)
                if sc > round_score:
                    round_best = cand
                    round_score = sc
        if round_best == current:
            break
        current = round_best
        try_add(current)

    if best and best != s:
        core = "".join(ch for ch in best if not ch.isspace())
        zh_n, common_n = cjk_stats(core)
        common_ratio = (common_n / zh_n) if zh_n else 0.0
        has_hint = any(k in best for k in RECOVER_HINTS)
        if core and cjk_ratio(best) >= 0.08 and zh_n >= 6 and (common_ratio >= 0.12 or has_hint):
            return best
    return None


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main() -> None:
    ap = argparse.ArgumentParser(description="Prune unreadable snippets from cleaned corpus.")
    ap.add_argument("--input-jsonl", required=True)
    ap.add_argument("--out-dir", default="/Users/shine/lottery_research/data")
    args = ap.parse_args()

    inp = Path(args.input_jsonl)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = [json.loads(l) for l in inp.open(encoding="utf-8") if l.strip()]

    kept: list[dict] = []
    removed: list[dict] = []

    for r in rows:
        snippet = str(r.get("snippet", "") or "")
        if not looks_unreadable_candidate(snippet):
            kept.append(r)
            continue

        recovered = try_recover(snippet)
        if recovered:
            r["snippet"] = recovered
            kept.append(r)
            continue

        removed.append(
            {
                "url": r.get("url", ""),
                "title": str(r.get("title", ""))[:300],
                "host": r.get("host", ""),
                "status": r.get("status", ""),
                "http_status": r.get("http_status", ""),
                "reason": "snippet全乱码且无法转码",
                "snippet_preview": snippet[:500],
            }
        )

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_jsonl = out_dir / f"正文库_清洗_去乱码_{ts}.jsonl"
    out_csv = out_dir / f"正文库_清洗_去乱码_{ts}.csv"
    removed_jsonl = out_dir / f"正文库_去乱码_删除明细_{ts}.jsonl"
    removed_csv = out_dir / f"正文库_去乱码_删除明细_{ts}.csv"
    summary_path = out_dir / f"正文库_去乱码_汇总_{ts}.json"

    with out_jsonl.open("w", encoding="utf-8") as f:
        for r in kept:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    with removed_jsonl.open("w", encoding="utf-8") as f:
        for r in removed:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    write_csv(
        out_csv,
        kept,
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

    write_csv(
        removed_csv,
        removed,
        [
            "url",
            "title",
            "host",
            "status",
            "http_status",
            "reason",
            "snippet_preview",
        ],
    )

    summary = {
        "input": str(inp),
        "input_total": len(rows),
        "kept_total": len(kept),
        "removed_total": len(removed),
        "outputs": {
            "cleaned_jsonl": str(out_jsonl),
            "cleaned_csv": str(out_csv),
            "removed_jsonl": str(removed_jsonl),
            "removed_csv": str(removed_csv),
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
