#!/usr/bin/env python3

import argparse
import pandas as pd

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", default="reddit_movie_posts_full.tsv", help="Input TSV")
    p.add_argument("--out", dest="out", default="open_coding_200.tsv", help="Output TSV")
    p.add_argument("--n", type=int, default=200, help="Sample size (default 200)")
    p.add_argument("--seed", type=int, default=42, help="Random seed (default 42)")
    args = p.parse_args()

    df = pd.read_csv(args.inp, sep="\t")
    if len(df) == 0:
        raise SystemExit("Input has 0 rows.")

    n = min(args.n, len(df))
    sample = df.sample(n=n, random_state=args.seed).copy()

    # add a blank 'topic' column at the end for coders
    sample["topic"] = ""

    sample.to_csv(args.out, sep="\t", index=False)
    print(f"Wrote {args.out} ({len(sample)} rows) from {args.inp}")

if __name__ == "__main__":
    main()
