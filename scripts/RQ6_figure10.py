import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# --- Load ---
df = pd.read_csv("total_comments_data.csv")
df = df[['repo_id', 'total_comments', 'label', 'push_bin']].dropna()

# --- Categorical ordering (as in the paper) ---
bucket_order = ['<5','<10','<50','<100','<500','<1k','<5k','<10k','<25k','<50k','<100k','<500k','1M+']
label_order  = ['english', 'mixed', 'non_english']

df = df[df['push_bin'].isin(bucket_order) & df['label'].isin(label_order)].copy()
df['push_bin'] = pd.Categorical(df['push_bin'], categories=bucket_order, ordered=True)
df['label']    = pd.Categorical(df['label'],    categories=label_order,  ordered=True)

# --- Group to lists for boxplots ---
grouped = df.groupby(['push_bin', 'label'])['total_comments'].apply(list)

# --- Positions: 3 boxplots per bucket (english, mixed, non_english) ---
num_groups = len(label_order)
width = 0.25  # spacing for groups within a bucket

positions = {}
for i, b in enumerate(bucket_order):
    base = i * (num_groups + 1) * width  # +1 gap between buckets
    for j, lab in enumerate(label_order):
        positions[(b, lab)] = base + j * width

data, pos = [], []
for b in bucket_order:
    for lab in label_order:
        data.append(grouped.get((b, lab), []))
        pos.append(positions[(b, lab)])




# --- Plot ---
plt.figure(figsize=(16, 6))
sns.set_palette("Set2")
# Prepare data for seaborn
plot_df = df.copy()

# Create boxplot using seaborn
sns.boxplot(
    data=plot_df,
    x='push_bin',
    y='total_comments',
    hue='label',
    order=bucket_order,
    hue_order=label_order,
    width=0.6,
    showfliers=False
)

plt.yscale('log')
plt.xlabel("GitHub Contributions Bucket", fontsize=18)
plt.ylabel("Total Comments", fontsize=18)
# plt.legend(title='Language', frameon=False, loc='upper left')
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3, fontsize=18)
plt.xticks(fontsize=16)
plt.yticks(fontsize=18)
plt.tight_layout()
out_path = "total_comments_per_repo_repro.pdf"
plt.savefig(out_path, bbox_inches='tight')
plt.show()

# print("Saved to:", out_path)
