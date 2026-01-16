import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
base_palette = sns.color_palette("deep", n_colors=10)  # 'deep' has 10 colors
# Add one more distinct dark color (e.g., dark teal)
extra_color = sns.dark_palette("teal", n_colors=1)[0]
excluded_languages = [
    'french',
    'spanish',
    'portuguese',
    'italian',
    'german',
    'dutch'
]
palette = [color for color in base_palette] + [extra_color]
# Load data
all_years = []
for year in range(2015, 2026):
    try:
        df = pd.read_csv(f'language_counts_by_month_{year}.csv')
        all_years.append(df)
    except FileNotFoundError:
        print(f"File missing for year {year}, skipping.")
        continue

# Combine all data
df_all = pd.concat(all_years, ignore_index=True)
df_all =  df_all[~df_all['language'].str.lower().isin(excluded_languages)]
# Total and non-English messages per date
total_per_date = df_all.groupby('date')['count'].sum()
non_english_df = df_all[df_all['language'].str.lower() != 'english']
# non_english_df = df_all[~df_all['language'].str.lower().isin(['english'] + excluded_languages)]
non_english_per_date = non_english_df.groupby('date')['count'].sum()

# Calculate percent
percent_non_english = (non_english_per_date / total_per_date) * 100
percent_non_english = percent_non_english.sort_index()

# Plot line connecting all points
plt.figure(figsize=(10, 4))
plt.plot(percent_non_english.index, percent_non_english.values, color='gray', linewidth=1)

# Add colored markers by year
colors = plt.cm.get_cmap('tab10', 11)
years = list(range(2015, 2026))

for idx, year in enumerate(years):
    year_str = str(year)
    year_data = percent_non_english[percent_non_english.index.str.startswith(year_str)]
    plt.scatter(
        year_data.index,
        year_data.values,
        color=palette[idx],
        label=year_str
    )

# plt.title('Percent of Non-English Messages per Month (2015–2025)')
plt.xlabel(None)
plt.yticks(fontsize=18)
plt.gca().yaxis.set_major_locator(plt.MaxNLocator(integer=True))
plt.ylabel("Non-English (%)", fontsize=20)
# Reduce xticks
xticks = percent_non_english.index.tolist()
step = 6
formatted_labels = [f"{d[2:4]}-{d[5:7]}" for d in xticks]  # convert "2020-03" -> "20-03"

plt.xticks(
    ticks=range(0, len(xticks), step),
    labels=[formatted_labels[i] for i in range(0, len(xticks), step)],
    rotation=90,
    fontsize=16
)
# plt.grid(True, linestyle='--', alpha=0.4)
plt.legend(
    fontsize=16,
    title_fontsize=22,
    ncol=3,
    loc="upper left",
    borderaxespad=0.5
)
plt.ylim(0,35)
plt.tight_layout()
plt.savefig("RQ1.pdf", bbox_inches="tight")
plt.show()