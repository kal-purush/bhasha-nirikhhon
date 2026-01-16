from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
from scipy.stats import kruskal, mannwhitneyu
import itertools
import seaborn as sns

# 1) Load
df = pd.read_csv("total_contributors_data.csv")

# Expect columns: repo_id, contributors_count, label, (optional) push_bin
# needed = {"repo_id", "contributors_count", "label",}
# missing = needed - set(df.columns)
# if missing:
#     raise ValueError(f"Missing columns: {missing}")


bins = [0, 5, 10, 50, 100, 500, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 500_000, float("inf")]
labels = ["<5", "<10", "<50", "<100", "<500", "<1k", "<5k", "<10k", "<25k", "<50k", "<100k", "<500k", "1M+"]

df["push_bucket"] = pd.cut(df["push_count"], bins=bins, labels=labels, include_lowest=True)

label_order  = ['english', 'mixed', 'non_english']

# df = df[df['push_bin'].isin(bucket_order) & df['label'].isin(label_order)].copy()
# df['push_bin'] = pd.Categorical(df['push_bin'], categories=bucket_order, ordered=True)
df['label']    = pd.Categorical(df['label'],    categories=label_order,  ordered=True)

# --- Step 4: Clean missing data & convert duration ---
df = df.dropna(subset=["contributors_count", "label", "push_bucket"])
# df["duration_days"] = df["duration_seconds"] / 86400  # convert seconds to days

# 2) Clean
# df = df[["repo_id", "contributors_count", "label"]].dropna()
# df = df[df["label"].isin(["english", "mixed", "non_english"])].copy()



plt.figure(figsize=(10,4))
sns.set_palette("Set2")
sns.boxplot(data=df, x="push_bucket", y="contributors_count", hue="label", showfliers=False)
plt.yscale("log")  # durations vary widely
plt.xlabel("GitHub Contributions Bucket", fontsize=18)
plt.ylabel("Total Contributors", fontsize=18)
# plt.title("Distribution of issue resolution duration by classification and repo activity")
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=3, fontsize=18)
plt.tight_layout()
plt.xticks(fontsize=16)
plt.yticks(fontsize=18)
plt.savefig("total_contributors_per_repo.pdf", bbox_inches='tight')
plt.show()