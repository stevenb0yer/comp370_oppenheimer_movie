#!/usr/bin/env python3

import time, re, requests, pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from langdetect import detect, LangDetectException

SUBREDDITS = ["movies", "flicks", "truefilm"]
REQUEST_DELAY = 0.8
HEADERS = {"User-Agent": "RedditMovieProject/1.0"}

MIN_TEXT_LEN = 15 

# end_utc=None means "open-ended" (from NOW back to start_utc)
MOVIES: Dict[str, Dict[str, Any]] = {
    "oppenheimer": {
        "query": '(oppenheimer) (film OR movie OR trailer OR review OR "box office")',
        "start_utc": 1688083200,  # 2023-06-30
        "end_utc":   None,
        "title_tokens": ["oppenheimer"],
        "negative_title_regex": None,
    },
    "barbie": {
        "query": '(barbie) (film OR movie OR trailer OR review OR "box office")',
        "start_utc": 1688083200,
        "end_utc":   None,
        "title_tokens": ["barbie"],
        "negative_title_regex": r'\b(doll|toy|birthday|ken doll|barbiecore)\b',
    },
    "mi7": {
        "query": '("mission impossible" OR "dead reckoning" OR "mi7") (film OR movie OR trailer OR review OR "box office")',
        "start_utc": 1688083200,
        "end_utc":   None,
        "title_tokens": ["mission impossible", "dead reckoning", "mi7"],
        "negative_title_regex": None,
    },
    "sound_of_freedom": {
        "query": '("sound of freedom") (film OR movie OR trailer OR review OR "box office")',
        "start_utc": 1688083200,
        "end_utc":   None,
        "title_tokens": ["sound of freedom"],
        "negative_title_regex": None,
    },
    "indiana_jones": {
        "query": '("dial of destiny" OR "indiana jones") (film OR movie OR trailer OR review OR "box office")',
        "start_utc": 1688083200,
        "end_utc":   None,
        "title_tokens": ["dial of destiny", "indiana jones"],
        "negative_title_regex": None,
    },
}

FULL_TSV = "reddit_movie_posts_full.tsv"
COVERAGE_SUMMARY = "coverage_summary.csv"
BASE_URL = "https://www.reddit.com/r/{}/search.json"

def now_utc_seconds() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def is_english(text: str) -> bool:
    if not text:
        return False
    txt = text.strip()
    if len(txt) >= MIN_TEXT_LEN:
        try:
            return detect(txt) == "en"
        except LangDetectException:
            pass
        except Exception:
            pass
    # for short text
    ascii_ratio = sum(1 for ch in txt if ord(ch) < 128) / max(1, len(txt))
    return ascii_ratio > 0.9

def has_film_cue(lower: str) -> bool:
    return bool(re.search(r'\b(film|movie|trailer|review|box\s*office|director|cast|cinema|screening|premiere)\b', lower))

def title_has_token(title_lower: str, tokens: List[str]) -> bool:
    return any(tok.lower() in title_lower for tok in tokens)

def should_exclude_by_negatives(title_lower: str, negatives_regex: Optional[str], film_cue_present: bool) -> bool:
    if not negatives_regex:
        return False
    if not re.search(negatives_regex, title_lower):
        return False
    return not film_cue_present  # allow if film cues present

def fetch_subreddit_posts(subreddit: str, query: str, start_utc: int) -> List[dict]:
    """Page 'new' results until crossing below start_utc."""
    posts, after = [], None
    while True:
        params = {"q": query, "restrict_sr": "on", "sort": "new", "limit": 100}
        if after:
            params["after"] = after
        try:
            r = requests.get(BASE_URL.format(subreddit), params=params, headers=HEADERS, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception:
            break
        children = data.get("data", {}).get("children", [])
        if not children:
            break
        posts.extend([c["data"] for c in children])

        after = data.get("data", {}).get("after")
        if not after:
            break

        # stop if the oldest post on this page is older than start
        created_times = [c["data"].get("created_utc", 0) for c in children if "data" in c]
        if created_times and min(created_times) < start_utc:
            break

        time.sleep(REQUEST_DELAY)
    return posts

def collect_for_movie(movie_key: str, cfg: Dict[str, Any]) -> List[dict]:
    all_posts = []
    for sr in SUBREDDITS:
        all_posts.extend(fetch_subreddit_posts(sr, cfg["query"], cfg["start_utc"]))
        time.sleep(REQUEST_DELAY)
    # dedupe by id
    return list({p.get("id"): p for p in all_posts if p.get("id")}.values())

def build_rows(movie_key: str, cfg: Dict[str, Any], raw_posts: List[dict]) -> List[dict]:
    rows = []
    start_utc = cfg["start_utc"]
    title_tokens = cfg["title_tokens"]
    neg_regex = cfg.get("negative_title_regex")

    kept = 0
    for p in raw_posts:
        created_utc = int(p.get("created_utc", 0))
        if created_utc < start_utc:
            continue

        title = (p.get("title") or "").strip()
        selftext = (p.get("selftext") or "").strip()
        combined = f"{title} {selftext}".strip()
        title_lower = title.lower()
        film_cue = has_film_cue(title_lower)

        # require movie token in title, or keep if body has token + title has film cue
        if not title_has_token(title_lower, title_tokens):
            body_lower = selftext.lower()
            if not (film_cue and any(tok.lower() in body_lower for tok in title_tokens)):
                continue

        if should_exclude_by_negatives(title_lower, neg_regex, film_cue):
            continue

        lang_text = title if len(selftext) < MIN_TEXT_LEN else combined
        if not is_english(lang_text):
            continue

        rows.append({
            "movie": movie_key,
            "subreddit": p.get("subreddit", ""),
            "id": p.get("id", ""),
            "created_utc": created_utc,
            "title": title,
            "text": selftext,
            "permalink": p.get("permalink", ""),
            "url": ("https://www.reddit.com" + p["permalink"]) if p.get("permalink") else "",
        })
        kept += 1

    print(f"[{movie_key}] kept {kept} posts")
    return rows

def summarize_rates(df_full: pd.DataFrame) -> pd.DataFrame:
    """Simple per-movie summary incl. posts/day since start (open-ended window)."""
    if df_full.empty:
        return pd.DataFrame(), pd.DataFrame()
    now_sec = now_utc_seconds()
    starts = {k: MOVIES[k]["start_utc"] for k in MOVIES}
    df_full = df_full.copy()
    df_full["days_since_start"] = df_full["movie"].map(lambda m: max(1, (now_sec - starts[m]) // 86400))
    by_movie = df_full.groupby("movie").agg(
        count=("id", "size"),
        days_since_start=("days_since_start", "max"),
    ).reset_index()
    by_movie["posts_per_day_since_start"] = by_movie["count"] / by_movie["days_since_start"]
    by_movie_sub = df_full.groupby(["movie","subreddit"]).size().rename("count").reset_index()
    return by_movie, by_movie_sub

def main():
    all_rows: List[dict] = []
    for movie_key, cfg in MOVIES.items():
        print(f"Collecting: {movie_key}")
        raw = collect_for_movie(movie_key, cfg)
        all_rows.extend(build_rows(movie_key, cfg, raw))

    if not all_rows:
        print("No rows collected.")
        return

    df_full = pd.DataFrame(all_rows).drop_duplicates(subset=["movie","id"]).reset_index(drop=True)
    df_full = df_full.sort_values(by=["movie","created_utc"], ascending=[True, False]).reset_index(drop=True)
    df_full.to_csv(FULL_TSV, sep="\t", index=False)
    print(f"Wrote: {FULL_TSV} ({len(df_full)} rows)")

    # small summary CSV
    by_movie, by_movie_sub = summarize_rates(df_full)
    with open(COVERAGE_SUMMARY, "w", encoding="utf-8") as f:
        f.write("# counts_by_movie_with_rates\n"); by_movie.to_csv(f, index=False)
        f.write("\n# counts_by_movie_and_subreddit\n"); by_movie_sub.to_csv(f, index=False)
    print(f"Wrote: {COVERAGE_SUMMARY}")

if __name__ == "__main__":
    main()
