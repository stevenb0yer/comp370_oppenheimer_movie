import pandas as pd

# Paths to your files
FULL_PATH = "reddit_movie_posts_full.tsv"
OPEN_CODING_PATH = "open_coding_200.tsv"
OUTPUT_PATH = "reddit_movie_posts_remaining.tsv"

# Read the full dataset and the 200 open-coding posts
full_df = pd.read_csv(FULL_PATH, sep="\t")
open_df = pd.read_csv(OPEN_CODING_PATH, sep="\t")

# Check which IDs are in the open-coding set
open_ids = set(open_df["id"])

# Keep only rows from the full dataset whose id is NOT in the 200
remaining_df = full_df[~full_df["id"].isin(open_ids)].copy()

print(f"Total posts in full dataset: {len(full_df)}")
print(f"Posts in open-coding sample: {len(open_df)}")
print(f"Remaining posts to annotate: {len(remaining_df)}")

# Save remaining posts to a new TSV
remaining_df.to_csv(OUTPUT_PATH, sep="\t", index=False)
print(f"Saved remaining posts to: {OUTPUT_PATH}")
