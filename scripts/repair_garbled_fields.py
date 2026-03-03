#!/usr/bin/env python3
"""Detect and repair mojibake in title/snippet/text fields."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from pathlib import Path


FIELDS = ["title", "snippet", "text"]

MOJIBAKE_LATIN_RE = re.compile(
    r"[ÃÂâäåæçèéêëìíîïðñòóôõöøùúûüýþÿÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]"
)
HIGH_LATIN_RE = re.compile(r"[À-ÿ]")
CJK_RE = re.compile(r"[\u4e00-\u9fff]")
PUA_RE = re.compile(r"[\ue000-\uf8ff]")
C1_RE = re.compile(r"[\u0080-\u009f]")
ARG1_RE = re.compile(r"var\s+arg1\s*=", re.IGNORECASE)
BASE64_LONG_RE = re.compile(r"[A-Za-z0-9+/]{90,}={0,2}")

COMMON_CJK = set(
    "的一是在不了有和人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动同工也能下过子说产种面而方后多定行学法所民得经十三之进着等部度家电力里如水化高自二理起小物现实加量都两体制机当使点从业本去把性好应开它合还因由其些然前外天政四日那社义事平形相全表间样与关各重新线内数正心反你明看原又么利比或但质气第向道命此变条只没结解问意建月公无系军很情者最立代想已通并提直题党程展五果料象员革位入常文总次品式活设及管特件长求老"
)

RECOVER_HINTS = [
    "双色球",
    "彩票",
    "福彩",
    "开奖",
    "论坛",
    "选号",
    "走势图",
    "中奖",
]

# High-confidence source encoding -> target decoding.
PRIMARY_TRANSFORMS = [
    ("latin1", "utf-8"),
    ("cp1252", "utf-8"),
    ("latin1", "gbk"),
    ("latin1", "gb18030"),
    ("cp1252", "gbk"),
    ("cp1252", "gb18030"),
]

ERROR_MODES = [
    ("strict", "strict"),
    ("ignore", "strict"),
    ("strict", "ignore"),
    ("ignore", "ignore"),
]


def cjk_stats(text: str) -> tuple[int, int]:
    z = CJK_RE.findall(text or "")
    if not z:
        return 0, 0
    common = sum(1 for ch in z if ch in COMMON_CJK)
    return len(z), common


def likely_garbled(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return False
    core = "".join(ch for ch in s if not ch.isspace())
    if not core:
        return False

    if "�" in s:
        return True
    if ARG1_RE.search(s) and BASE64_LONG_RE.search(s):
        return True

    moj = len(MOJIBAKE_LATIN_RE.findall(core))
    c1 = len(C1_RE.findall(core))
    pua = len(PUA_RE.findall(core))
    cjk_n, cjk_common = cjk_stats(core)
    cjk_common_ratio = (cjk_common / cjk_n) if cjk_n else 0.0
    moj_ratio = moj / len(core)
    has_hint = any(k in s for k in RECOVER_HINTS)
    ascii_words = re.findall(r"[A-Za-z]{3,}", s)
    ascii_word_len = sum(len(w) for w in ascii_words)

    # Readable lottery pages should not be flagged only because of uncommon CJK.
    if moj == 0 and pua == 0 and has_hint and cjk_n >= 2 and "�" not in s:
        return False
    # Multilingual app/store text may contain accents but is not mojibake.
    if c1 == 0 and moj_ratio < 0.12 and len(ascii_words) >= 4 and ascii_word_len >= 22:
        return False

    if pua > 0 and (c1 > 0 or moj_ratio > 0.04):
        return True
    if c1 > 0:
        return True
    if moj_ratio > 0.12 and moj >= 4:
        return True
    if moj_ratio > 0.05 and moj >= 4 and cjk_n >= 4:
        return True
    if cjk_n >= 12 and cjk_common_ratio < 0.02:
        return True
    if len(HIGH_LATIN_RE.findall(core)) >= 8 and moj >= 3:
        return True
    return False


def text_score(text: str) -> float:
    s = text or ""
    core = "".join(ch for ch in s if not ch.isspace())
    if not core:
        return -100.0

    n = len(core)
    cjk_n, cjk_common = cjk_stats(core)
    cjk_common_ratio = (cjk_common / cjk_n) if cjk_n else 0.0

    moj = len(MOJIBAKE_LATIN_RE.findall(core))
    c1 = len(C1_RE.findall(core))
    pua = len(PUA_RE.findall(core))
    repl = s.count("�")
    ctrl = sum(1 for ch in core if ord(ch) < 32)
    alnum = sum(1 for ch in core if ch.isalnum())
    digits = sum(1 for ch in core if ch.isdigit())
    punct = sum(1 for ch in core if (not ch.isalnum()) and (ord(ch) >= 32))
    has_hint = any(k in s for k in RECOVER_HINTS)

    score = 0.0
    score += cjk_common * 4.0
    score += cjk_n * 0.35
    score += (alnum / n) * 6.0
    score += max(0.0, 2.0 - (punct / n) * 6.0)
    score += 3.0 if has_hint else 0.0

    if cjk_n >= 10 and cjk_common_ratio < 0.05:
        score -= 26.0
    if ARG1_RE.search(s) and BASE64_LONG_RE.search(s):
        score -= 100.0

    digit_ratio = digits / n
    if n >= 30 and digit_ratio > 0.30:
        score -= (digit_ratio - 0.30) * 140.0

    moj_ratio = moj / n
    if moj_ratio > 0.15:
        score -= moj * 2.0
    elif moj_ratio > 0.08:
        score -= moj * 1.2
    else:
        score -= moj * 0.2
    score -= c1 * 5.0
    score -= pua * 0.6
    score -= repl * 5.0
    score -= ctrl * 8.0
    return score


def maybe_transform(text: str, src_enc: str, dst_enc: str, enc_err: str, dec_err: str) -> str | None:
    try:
        out = text.encode(src_enc, errors=enc_err).decode(dst_enc, errors=dec_err)
    except Exception:
        return None
    if not out:
        return None
    return out


def repair_text(text: str) -> tuple[str, bool, bool, float]:
    original = text or ""
    garbled = likely_garbled(original)
    if not original:
        return original, garbled, False, 0.0
    if not garbled:
        return original, garbled, False, 0.0

    best = original
    best_score = text_score(original)
    original_score = best_score
    orig_cjk_n, _ = cjk_stats(original)

    current = original
    for _ in range(4):
        round_best = current
        round_best_score = text_score(current)
        cur_core = "".join(ch for ch in current if not ch.isspace())
        cur_moj = len(MOJIBAKE_LATIN_RE.findall(cur_core))
        cur_moj_ratio = (cur_moj / len(cur_core)) if cur_core else 0.0
        cur_digits = sum(1 for ch in cur_core if ch.isdigit())
        cur_digit_ratio = (cur_digits / len(cur_core)) if cur_core else 0.0
        min_keep_ratio = 0.24 if cur_moj_ratio > 0.06 else 0.55

        for src, dst in PRIMARY_TRANSFORMS:
            for enc_err, dec_err in ERROR_MODES:
                cand = maybe_transform(current, src, dst, enc_err, dec_err)
                if cand is None or cand == current:
                    continue

                # Reject over-destructive transforms.
                core_now = len("".join(ch for ch in current if not ch.isspace()))
                core_new = len("".join(ch for ch in cand if not ch.isspace()))
                if core_now > 0 and core_new < core_now * min_keep_ratio:
                    continue

                cand_core = "".join(ch for ch in cand if not ch.isspace())
                cand_digits = sum(1 for ch in cand_core if ch.isdigit())
                cand_digit_ratio = (cand_digits / len(cand_core)) if cand_core else 0.0
                cand_cjk_n, _ = cjk_stats(cand)
                cand_has_hint = any(k in cand for k in RECOVER_HINTS)

                # Avoid converting readable Chinese into numeric garbage.
                if orig_cjk_n >= 8 and cand_cjk_n < 3 and not cand_has_hint:
                    continue
                # Reject number-like garbage only when Chinese coverage is very low.
                if len(cand_core) >= 30 and cand_digit_ratio > 0.85 and cand_cjk_n < 50 and not cand_has_hint:
                    continue

                sc = text_score(cand)
                delta = 0.8 if (enc_err == "strict" and dec_err == "strict") else 2.4
                if sc > round_best_score + delta:
                    round_best = cand
                    round_best_score = sc

        if round_best == current:
            break
        current = round_best
        if round_best_score > best_score:
            best = round_best
            best_score = round_best_score

    # Final acceptance: either score improves enough or we recover domain hints.
    improved = best_score > original_score + 1.2
    if improved:
        return best, garbled, True, (best_score - original_score)
    return original, garbled, False, 0.0


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def main() -> None:
    ap = argparse.ArgumentParser(description="Repair garbled title/snippet/text fields.")
    ap.add_argument("--input-jsonl", required=True)
    ap.add_argument("--out-dir", default="/Users/shine/lottery_research/data")
    args = ap.parse_args()

    inp = Path(args.input_jsonl)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = [json.loads(l) for l in inp.open(encoding="utf-8") if l.strip()]

    repaired_rows: list[dict] = []
    detail_rows: list[dict] = []

    title_garbled_cnt = 0
    title_repaired_cnt = 0
    field_garbled_cnt = {k: 0 for k in FIELDS}
    field_repaired_cnt = {k: 0 for k in FIELDS}
    unresolved_rows = 0

    for r in rows:
        row = dict(r)
        url = str(row.get("url", ""))

        per = {}
        for f in FIELDS:
            before = str(row.get(f, "") or "")
            after, garbled, repaired, gain = repair_text(before)
            per[f] = {
                "before": before,
                "after": after,
                "garbled": garbled,
                "repaired": repaired,
                "gain": gain,
            }
            if garbled:
                field_garbled_cnt[f] += 1
            if repaired:
                field_repaired_cnt[f] += 1
                row[f] = after

        title_is_garbled = per["title"]["garbled"]
        row["title_is_garbled"] = "yes" if title_is_garbled else "no"
        if title_is_garbled:
            title_garbled_cnt += 1
        if per["title"]["repaired"]:
            title_repaired_cnt += 1

        unresolved_fields = [f for f in FIELDS if per[f]["garbled"] and not per[f]["repaired"]]
        if unresolved_fields:
            unresolved_rows += 1

        if (
            per["title"]["garbled"]
            or per["snippet"]["garbled"]
            or per["text"]["garbled"]
            or per["title"]["repaired"]
            or per["snippet"]["repaired"]
            or per["text"]["repaired"]
        ):
            detail_rows.append(
                {
                    "url": url,
                    "title_is_garbled": "yes" if per["title"]["garbled"] else "no",
                    "title_repaired": "yes" if per["title"]["repaired"] else "no",
                    "snippet_repaired": "yes" if per["snippet"]["repaired"] else "no",
                    "text_repaired": "yes" if per["text"]["repaired"] else "no",
                    "unresolved_fields": ",".join(unresolved_fields),
                    "title_before": per["title"]["before"][:300],
                    "title_after": per["title"]["after"][:300],
                }
            )

        repaired_rows.append(row)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_jsonl = out_dir / f"正文库_清洗_全字段转码_{ts}.jsonl"
    out_csv = out_dir / f"正文库_清洗_全字段转码_{ts}.csv"
    detail_csv = out_dir / f"正文库_乱码识别与修复明细_{ts}.csv"
    detail_jsonl = out_dir / f"正文库_乱码识别与修复明细_{ts}.jsonl"
    summary_path = out_dir / f"正文库_全字段转码汇总_{ts}.json"

    with out_jsonl.open("w", encoding="utf-8") as f:
        for row in repaired_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    with detail_jsonl.open("w", encoding="utf-8") as f:
        for row in detail_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    write_csv(
        out_csv,
        repaired_rows,
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
        ],
    )

    write_csv(
        detail_csv,
        detail_rows,
        [
            "url",
            "title_is_garbled",
            "title_repaired",
            "snippet_repaired",
            "text_repaired",
            "unresolved_fields",
            "title_before",
            "title_after",
        ],
    )

    summary = {
        "input": str(inp),
        "input_total": len(rows),
        "title_garbled_count": title_garbled_cnt,
        "title_repaired_count": title_repaired_cnt,
        "field_garbled_count": field_garbled_cnt,
        "field_repaired_count": field_repaired_cnt,
        "unresolved_rows": unresolved_rows,
        "outputs": {
            "repaired_jsonl": str(out_jsonl),
            "repaired_csv": str(out_csv),
            "detail_csv": str(detail_csv),
            "detail_jsonl": str(detail_jsonl),
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
