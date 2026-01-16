import pandas as pd
import matplotlib.pyplot as plt

# Load Dataset
file_path = 'global-data-on-sustainable-energy.csv'  # Update with the actual path to your CSV file
data = pd.read_csv(file_path)

threshold = len(data) * 0.5  # 50% missing threshold
cleaned_data = data.dropna(thresh=threshold, axis=1).copy()

numerical_columns = cleaned_data.select_dtypes(include=['float64', 'int64']).columns
cleaned_data.loc[:, numerical_columns] = cleaned_data[numerical_columns].fillna(cleaned_data[numerical_columns].median())

categorical_columns = cleaned_data.select_dtypes(include=['object']).columns
cleaned_data.loc[:, categorical_columns] = cleaned_data[categorical_columns].fillna("Unknown")

electricity_trend = cleaned_data.groupby('Year')['Access to electricity (% of population)'].mean()

plt.figure(figsize=(12, 6))
plt.plot(electricity_trend.index, electricity_trend.values, marker='o', color='blue', label='Access to Electricity')
plt.title("Global Access to Electricity Over Time", fontsize=16)
plt.xlabel("Year", fontsize=14)
plt.ylabel("Access to Electricity (% of population)", fontsize=14)
plt.grid(True)
plt.legend()
plt.show()