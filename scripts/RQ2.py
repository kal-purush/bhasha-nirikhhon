import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
import matplotlib.patches as mpatches
import numpy as np
from matplotlib.ticker import FuncFormatter
import matplotlib.patches as mpatches
import seaborn as sns
base_palette = sns.color_palette("deep", n_colors=10)  # 'deep' has 10 colors
# Add one more distinct dark color (e.g., dark teal)
extra_color = sns.dark_palette("teal", n_colors=1)[0]

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# List of languages to exclude (lowercased for consistency)
excluded_languages = [
    'french',
    'spanish',
    'portuguese',
    'italian',
    'german',
    'dutch'
]

all_years = []
for year in range(2015, 2026):
    try:
        df = pd.read_csv(f'language_counts_by_month_{year}.csv')
        all_years.append(df)
    except Exception as e:
        print(f"Error processing file for year {year}: {e}")
        continue

# Combine all years
df_all = pd.concat(all_years, ignore_index=True)

# Get total messages per date (including English)
total_per_date = df_all.groupby('date')['count'].sum()

# Filter out English and the excluded languages
df_filtered = df_all[~df_all['language'].str.lower().isin(['english'] + excluded_languages)]

# Get top 10 languages after filtering
top_langs = (
    df_filtered.groupby('language')['count']
    .sum()
    .sort_values(ascending=False)
    .head(10)
    .index.tolist()
)

# Keep only those top 10
df_top = df_filtered[df_filtered['language'].isin(top_langs)]

# Pivot: date x language
pivot_df = df_top.pivot(index='date', columns='language', values='count').fillna(0)

# Normalize per total messages (including English + excluded languages)
percent_df = pivot_df.div(total_per_date, axis=0) * 100

# Sort by date
percent_df = percent_df.sort_index()

# Plot
# percent_df.plot(kind='bar', stacked=True, figsize=(16, 7))

# plt.title('Top 10 Non-English, Non-Western European Languages (% of All Messages, 2015–2025)')
# plt.xlabel('Month')
# plt.ylabel('Percentage')
# plt.xticks(rotation=90)
# plt.legend(title='Language', bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.tight_layout()
# plt.show()

base_palette = sns.color_palette("deep", n_colors=10)  # 'deep' has 10 colors
# Add one more distinct dark color (e.g., dark teal)
extra_color = sns.dark_palette("teal", n_colors=1)[0]

palette = [color for color in base_palette] + [extra_color]
plt.figure(figsize=(12, 5))

# Define hatching patterns for up to 11 bars
hatch_patterns = ['', '//', '\\\\', 'xx', '++', '..', '**', 'oo', 'OO', '--', '||']

# Plot the stacked bar with patterns
bars = percent_df.plot(
    kind='bar',
    stacked=True,
    color=palette,
    width=0.9,
    ax=plt.gca()
)

# Apply hatching patterns to each bar group
for i, bar_container in enumerate(bars.containers):
    hatch = hatch_patterns[i % len(hatch_patterns)]
    for rect in bar_container:
        rect.set_hatch(hatch)


plt.xlabel(None)
plt.ylabel("Non-English Messages (%)", fontsize=20)
all_xticks = list(percent_df.index)
step = max(1, len(all_xticks) // 20)
# Shorten year: '2015-01' -> '15-01'
short_labels = [x[2:] if len(x) >= 5 else x for x in all_xticks]
plt.xticks(
    ticks=range(0, len(all_xticks), step),
    labels=[short_labels[i] for i in range(0, len(short_labels), step)],
    rotation=90,
    fontsize=16
)
def millions(x, pos):
    return f'{x/1e6:.1f}M' if x >= 1e6 else (f'{x/1e3:.0f}k' if x >= 1e3 else str(int(x)))
# plt.gca().yaxis.set_major_formatter(FuncFormatter(millions))
plt.yticks(fontsize=16)

# plt.legend(bbox_to_anchor=(0.5, 1.12), loc='center', ncol = 5, fontsize=16)
plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.33), ncol=5, fontsize=17)
plt.tight_layout()
plt.savefig("RQ2.pdf", bbox_inches="tight")
plt.show()
#
