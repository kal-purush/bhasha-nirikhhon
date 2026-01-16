import pandas as pd
import matplotlib.pyplot as plt

# Load the file
df = pd.read_csv('language_detection_documentation.csv')

# Convert 'date' to datetime and extract year-month
df['date'] = pd.to_datetime(df['date'])
df['year_month'] = df['date'].dt.to_period('M')
df['language_clean'] = df['language'].str.strip("{}").str.lower().str.replace("'", "").str.split(', ')
# print(df[:50])
# # Mark as English if 'english' is in the list, otherwise Non-English
# df['lang_group'] = df['language_clean'].apply(lambda langs: 'English' if 'english' in langs else 'Non-English')
df['lang_group'] = df['language_clean'].apply(
    lambda langs: 'English' if isinstance(langs, list) and len(langs) == 1 and langs[0] == 'english' else 'Non-English'
)
# print(df[:50])
# # Count per month
counts = df.groupby(['year_month', 'lang_group']).size().unstack(fill_value=0).reset_index()

# Fill missing columns if needed
if 'English' not in counts.columns:
    counts['English'] = 0
if 'Non-English' not in counts.columns:
    counts['Non-English'] = 0

# Calculate percentage of Non-English
counts['Non_English_Perc'] = counts['Non-English'] / (counts['English'] + counts['Non-English']) * 100

# Sort by date
counts = counts.sort_values('year_month')

# Plot
plt.figure(figsize=(10, 4))
plt.bar(counts['year_month'].astype(str), counts['Non_English_Perc'])
plt.ylabel('Non-English \nRepository Share (%)', fontsize=18)
# plt.title('Non-English Percentage per Month')
# Use fewer xticks for clarity
all_xticks = counts['year_month'].astype(str).tolist()
step = max(1, len(all_xticks) // 12)  # Show ~12 ticks
plt.xticks(
    ticks=range(0, len(all_xticks), step),
    labels=[all_xticks[i] for i in range(0, len(all_xticks), step)],
    rotation=90,
    fontsize=16
)
plt.yticks(fontsize=18)
plt.tight_layout()
plt.savefig('documentation_non_english_percentage_per_month.pdf', dpi=1200)
plt.show()
