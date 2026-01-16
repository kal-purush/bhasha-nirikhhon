import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns

matplotlib.rcParams['figure.figsize'] = (20, 10)    # Set default figure size (width, height)
matplotlib.rcParams['font.size'] = 12               # Set default font size
matplotlib.rcParams['axes.titlesize'] = 20          # Set default title font size
matplotlib.rcParams['axes.labelsize'] = 20          # Set default label font size
plt.style.use('Solarize_Light2')                    # Set default style

# Read 2023 csv files competitions (Only Bouldering)
boulder_df = pd.read_csv("./competitions/world_cups_2023/2023_boulder.csv")

# Read the athletes file
athletes = pd.read_csv("./athlete_info_v2.csv")

athletes.head(10)

athletes.shape

athletes.dtypes

# Check the columns with '-' in it
(athletes == '-').any()

athletes[athletes['Active'] == '-']

# Remove the above value
idx = athletes[athletes['Active'] == '-'].index
athletes = athletes.drop(idx)

# Change data types from Object to Numeric type
athletes['Height'] = pd.to_numeric(athletes['Height'], errors='coerce')
athletes['Active'] = athletes['Active'].astype('Int64')

# Check the columns with '-' in it
(athletes == '-').any()

athletes.dtypes

athletes.isna().sum()

# There are 424 records with no height. Use mean value per gender and country to fill in missing values.
athletes_v2 = athletes
athletes_v2['Height'] = athletes['Height'].fillna(athletes.groupby(['Country', 'Gender'])['Height'].transform('mean'))
athletes_v2.isna().sum()

# Still missing some values. Use mean value per Gender.
athletes_v2['Height'] = athletes_v2['Height'].fillna(athletes.groupby(['Gender'])['Height'].transform('mean'))
athletes_v2.isna().sum()

boulder_df.head(10)

# Change columns names
boulder_df = boulder_df.rename(columns={"Name": "FName", "Unnamed: 2":"LName"})

# Explore the 'event_name' column
boulder_df['event_name'].value_counts()

# Issue with Seoul 2023 final round
boulder_df[boulder_df['event_name'] == '28 April - 30 April 2023']

# After checking sources, shift values to fix data for Seoul 2023 final
starting_row = 167
ending_row = 331

# Shift columns
boulder_df.iloc[starting_row:ending_row+1, 7:] = boulder_df.iloc[starting_row:ending_row+1, 6:-1].values
boulder_df.loc[starting_row:ending_row, 'Final'] = boulder_df.loc[starting_row:ending_row,'Semi-final']

boulder_df.isna().sum()

boulder_df.iloc[starting_row:ending_row]

boulder_df['event_name'].value_counts()

# Normalize event names to city_year
def normalize_event_name(event_name):
   return event_name.split("_", 3)[2:][1]

boulder_df['event_name'] = boulder_df['event_name'].apply(normalize_event_name)
boulder_df['event_name'].value_counts()

# Count number of 'dns'
boulder_df.eq('dns').sum()

# Remove athletes with 'dns' status in qualification column
boulder_df = boulder_df[boulder_df['Qualification'] != 'dns']

# Make new column with full name
boulder_df['Name'] = boulder_df['FName'] + " " + boulder_df['LName']

# Make new column for date type
boulder_df['Date1'] = boulder_df['Date'].str.split("-", n=1, expand=True)[1]
boulder_df['Date1'] = boulder_df['Date1'].str.strip()
boulder_df['Date1'] = pd.to_datetime(boulder_df['Date1'], format="%d %B %Y")

# Remove Date Column
boulder_df = boulder_df.drop(columns=['Date', 'FName', 'LName'])

boulder_df.isna().sum()

# Athlete exploratory data analysis
athletes_v2.shape
athletes_v2.columns
athletes_v2.describe()

# Age boxplot
plt.subplot(2, 1, 1)
sns.boxplot(data=athletes_v2, x='Age', hue='Gender')
plt.title('Boxplot for Age')

# Height boxplot
plt.subplot(2, 1, 2)
sns.boxplot(data=athletes_v2, x='Height', hue='Gender')
plt.title('Boxplot for Height')

plt.subplots_adjust(hspace=0.3)

plt.savefig(f'graphs/01_athlete_boxplot.png')
plt.show()

# Count Men and Women athletes
print(athletes_v2['Gender'].value_counts())
print(athletes_v2['Gender'].value_counts(normalize=True))

# Count participating countries
print("Total participating countries during the 2023 world cups:", len(athletes_v2['Country'].unique()))

# Top 10 countries with most participating athletes
athletes_v2['Country'].value_counts().head(10)

data = athletes_v2['Country'].value_counts()
title = "Athlete Count per Country During the 2023 Sport Climbing Competitions"

ax = sns.barplot(x=data.index, y=data.values)

for c in ax.containers:
    ax.bar_label(c)

plt.title(title)
plt.ylabel("")
plt.xlabel("")
plt.yticks([])
plt.xticks(rotation=45)

title = title.replace(" ", "_")
plt.savefig(f'graphs/02_{title}.png')
plt.show()

# Boulder EDA
boulder_df.shape

boulder_events_data = boulder_df.groupby(['event_name', 'Date1', 'Gender'])['Country'].count().reset_index().sort_values(by="Date1")
data = boulder_events_data
title = "Athlete Count in Bouldering by Event and Gender"

ax = sns.barplot(x='event_name', y='Country', hue='Gender', data=data)

for c in ax.containers:
    ax.bar_label(c)

plt.title(title)
plt.ylabel("Athletes")
plt.xlabel("Event")
plt.yticks([])

# Save the plot as a PNG file
title = title.replace(" ", "_")
plt.savefig(f'graphs/03_{title}.png')
plt.show()

# Number of medals in Boulder only
boulder_podium = boulder_df[(boulder_df['Rank'] == 1) | (boulder_df['Rank'] == 2) | (boulder_df['Rank'] == 3)].reset_index(drop=True)
boulder_podium['Category'] = 'Boulder'

# Get the number of medals won by country in Boulder
data = boulder_podium.groupby(['Country'])['Rank'].count().sort_values(ascending=False).reset_index()
title = "Total Medals Won by Country in Bouldering"

ax = sns.barplot(x='Country', y='Rank', data=data)

for c in ax.containers:
    ax.bar_label(c)

plt.title(title)
plt.xlabel("Country")
plt.ylabel("Medals Won")
plt.yticks([])

# Save the plot as a PNG file
title = title.replace(" ", "_")
plt.savefig(f'graphs/07_{title}.png')
plt.show()

# Get the medals won by gender in Boulder
data = boulder_podium.groupby(['Country', 'Gender'])['Rank'].count().reset_index().sort_values(by='Rank' ,ascending=False)
title = "Total Medals Won by Gender in Bouldering"

ax = sns.barplot(x='Country', y='Rank', hue='Gender', data=data)

for c in ax.containers:
    ax.bar_label(c)

plt.title(title)
plt.xlabel("")
plt.ylabel("Medals Won")
plt.yticks([])

# Save the plot as a PNG file
title = title.replace(" ", "_")
plt.savefig(f'graphs/08_{title}.png')
plt.show()

# Make column with Gold, Silver, Bronze
boulder_podium['Medal'] = boulder_podium['Rank'].map({1: 'Gold', 2:'Silver', 3: 'Bronze'})
medal_order =['Gold', 'Silver', 'Bronze']
boulder_podium['Medal'] = pd.Categorical(boulder_podium['Medal'], categories=medal_order, ordered=True)
data = boulder_podium.groupby(['Country', 'Medal'], observed=True)['Rank'].count().reset_index().sort_values(by='Rank', ascending=False)

title = "Medals Won by Country in Bouldering"
color_palette = {'Gold': 'gold', 'Silver': 'silver', 'Bronze': '#cd7f32'}
ax = sns.barplot(x='Country', y='Rank', hue='Medal', data=data, palette=color_palette)

for c in ax.containers:
    ax.bar_label(c)

plt.title(title)
plt.xlabel("Country")
plt.ylabel("Medals Won")
plt.yticks([])

# Save the plot as a PNG file
title = title.replace(" ", "_")
plt.savefig(f'graphs/10_{title}.png')
plt.show()

# Boulder Problems solved analysis
boulder_df['QT'] = boulder_df['Qualification'].str.split("t", n=1, expand=True)[0].astype('Int32')
boulder_df['QZ'] = boulder_df['Qualification'].str.split("t", n=1, expand=True)[1].str.split("z", n=1, expand=True)[0].astype('Int32')

boulder_df['SMT'] = boulder_df['Semi-final'].str.split("t", n=1, expand=True)[0].astype('Int32')
boulder_df['SMZ'] = boulder_df['Semi-final'].str.split("t", n=1, expand=True)[1].str.split("z", n=1, expand=True)[0].astype('Int32')

boulder_df['FT'] = boulder_df['Final'].str.split("t", n=1, expand=True)[0].astype('Int32')
boulder_df['FZ'] = boulder_df['Final'].str.split("t", n=1, expand=True)[1].str.split("z", n=1, expand=True)[0].astype('Int32')

# Remove Seoul 2023 boulder competition since the final was cancelled
b_final_top_subset = boulder_df[~(boulder_df['event_name'] == 'Seoul_2023')]

# Total Boulder TOPS
plt.subplot(2, 1, 1)
data_top = b_final_top_subset.groupby(['Gender', 'FT'])['Country'].count().reset_index()
title_top = "Total Boulder TOPS Reached During the Finals Competitions"
ax_top = sns.barplot(data=data_top, x='FT', y='Country', hue='Gender')

for c in ax_top.containers:
    ax_top.bar_label(c)

ax_top.set_title(title_top)
ax_top.set_ylabel("Number of Athletes")
ax_top.set_xlabel("Tops")
ax_top.set_yticks([])

# Total Boulder ZONES
plt.subplot(2, 1, 2)
b_final_zones_subset = boulder_df[~(boulder_df['event_name'] == 'Seoul_2023')]
data_zones = b_final_zones_subset.groupby(['Gender', 'FZ'])['Country'].count().reset_index()
title_zones = "Total Boulder ZONES Reached During the Finals Competitions"
ax_zones = sns.barplot(data=data_zones, x='FZ', y='Country', hue='Gender')

for c in ax_zones.containers:
    ax_zones.bar_label(c)

ax_zones.set_title(title_zones)
ax_zones.set_ylabel("Number of Athletes")
ax_zones.set_xlabel("Zones")
ax_zones.set_yticks([])

plt.subplots_adjust(hspace=0.3)
plt.savefig(f'graphs/11_Boulder_Final_Tops_Zones.png')
plt.show()

# Check flashed boulders in each round
boulder_df[boulder_df['Qualification'] == '5t5z 5 5'][['Rank', 'Name', 'Country', 'event_name']]
boulder_df[boulder_df['Semi-final'] == '4t4z 4 4'][['Rank', 'Name', 'Country', 'event_name']]
boulder_df[boulder_df['Final'] == '4t4z 4 4'][['Rank', 'Name', 'Country', 'event_name']]

# Correlation analysis between participation and finals in Boulder
dfs = boulder_df[['Rank', 'Name', 'Gender','event_name']]
# Convert Name to upper case
athletes_v2['Name'] = athletes_v2['Name'].str.upper()

merged_df = pd.merge(dfs, athletes_v2, on='Name', how='left')
merged_df.isna().sum()

merged_df = merged_df.dropna()
merged_df.head(10)

merged_df = merged_df.drop(columns=['AthleteUrl', 'Gender_y', 'Active'])
merged_df.head(10)

from scipy.stats import pearsonr

# Create a binary variable for reaching the finals in Boulder
# In Boulder finals, rank <= 6 generally means finalist
merged_df['Reached_Finals'] = merged_df['Rank'].apply(lambda x: 1 if x <= 6 else 0)

# Calculate Pearson correlation coefficient and p-value between Participations and Reached_Finals
correlation_coefficient, p_value = pearsonr(merged_df['Participations'], merged_df['Reached_Finals'])

print(f"Pearson Correlation Coefficient: {correlation_coefficient}")
print(f"P-value: {p_value}")

# Does Height matter? (Boulder only)
df_men = merged_df[merged_df['Gender_x'] == 'Men']
df_women = merged_df[merged_df['Gender_x'] == 'Women']

correlation_coefficient_men, p_value_men = pearsonr(df_men['Height'], df_men['Reached_Finals'])
print(f"Men - Pearson Correlation Coefficient: {correlation_coefficient_men}, P-value: {p_value_men}")

correlation_coefficient_women, p_value_women = pearsonr(df_women['Height'], df_women['Reached_Finals'])
print(f"Women - Pearson Correlation Coefficient: {correlation_coefficient_women}, P-value: {p_value_women}")