import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- Step 1: Read data ---
df = pd.read_csv("common_with_duration_push_classification.csv")

# --- Step 2: Normalize classification ---
def normalize_classification(x):
    if isinstance(x, str):
        x_lower = x.strip().lower()
        if "mixed" in x_lower:
            return "mixed"
        elif "english" in x_lower:
            return "english"
        else:
            return "non-english"
    return "non-english"

df["classification"] = df["classification"].apply(normalize_classification)

# --- Step 3: Define push_count buckets ---
bins = [0, 5, 10, 50, 100, 500, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 500_000, float("inf")]
labels = ["<5", "<10", "<50", "<100", "<500", "<1k", "<5k", "<10k", "<25k", "<50k", "<100k", "<500k", "1M+"]

df["push_bucket"] = pd.cut(df["push_count"], bins=bins, labels=labels, include_lowest=True)

# --- Step 4: Clean missing data & convert duration ---
df = df.dropna(subset=["duration_seconds", "classification", "push_bucket"])
df["duration_days"] = df["duration_seconds"] / 86400  # convert seconds to days

plt.figure(figsize=(10,4))
sns.set_palette("Set2")
sns.boxplot(data=df, x="push_bucket", y="duration_days", hue="classification", showfliers=False)
plt.yscale("log")  # durations vary widely
plt.xlabel("GitHub Contributions Bucket", fontsize=18)
plt.ylabel("Issue Resolution\nTime (days)", fontsize=18)
# plt.title("Distribution of issue resolution duration by classification and repo activity")
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=3, fontsize=18)
plt.tight_layout()
plt.xticks(fontsize=16)
plt.yticks(fontsize=18)
plt.savefig("issue_resolution_time_by_classification_and_repo_activity.pdf", bbox_inches='tight')
plt.show()