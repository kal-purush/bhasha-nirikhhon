import pandas as pd
import re

layoff_data = pd.read_excel(
    "../../data/Raw/Layoff/WARN Database 10-31-2023 [TO EDIT_ FILE-_MAKE A COPY].xlsx"
)

# drop redundant columns
layoff_data.drop(
    ["Temporary/Permanent", "Union", "Region", "Notes"], inplace=True, axis=1
)

# drop rows missing the number of employees affected column
layoff_data.dropna(subset=["Number of Workers"], inplace=True)

# Clean the effective date column
delimiters_pattern = r"[-,;&]|to|and"


# Function to extract the closing end of the date range
def extract_closing_date(date_range):
    try:
        if ("-" in date_range) or ("," in date_range) or ("to" in date_range):
            end_date_str = re.split(delimiters_pattern, date_range)[1].strip()
            return pd.to_datetime(end_date_str, errors="ignore")
    except:
        return pd.to_datetime(date_range, errors="ignore")


# Apply the function to the 'Effective\n Date' column
layoff_data["effective_date_cleaned"] = layoff_data["Effective\n Date"].apply(
    extract_closing_date
)

# Next we clean the indusry column
layoff_data["industry_cleaned"] = layoff_data["Industry"].replace(
    ["-", "–", "—"], "-", regex=True
)

# Identify records with the "number - industry name" format
pattern = r"^(\d+)\s*-\s*(.*)$"
mask = layoff_data["industry_cleaned"].str.match(pattern).fillna(False)

# Extract the mapping from the identified records
mapping = (
    layoff_data.loc[mask, "industry_cleaned"]
    .str.extract(pattern)
    .dropna()
    .set_index(0)[1]
    .to_dict()
)

numeric_mapping = {int(key): value for key, value in mapping.items()}

# Impute industry names for records that are simply numbers using the extracted mappings
layoff_data["industry_cleaned"] = layoff_data["industry_cleaned"].apply(
    lambda x: numeric_mapping.get(x, x)
)

# Convert records with "number - industry" format to "industry" format
layoff_data["industry_cleaned"] = layoff_data["industry_cleaned"].replace(
    pattern, r"\2", regex=True
)

# Save the processed output
layoff_data.to_excel("../../data/Clean/layoff_cleaned.xlsx")