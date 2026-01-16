from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64

app = Flask(__name__)

# Load the data from main.py
df = pd.read_pickle('data.pkl')  #  the DataFrame in main.py as a pickle file

# Gender diversity
gender_counts = df['gender'].value_counts(dropna=False).replace(
    {'Q6581097': 'Male', 'Q6581072': 'Female', 'Q1052281': 'Transgender female', 'Q2449503': 'Transgender male',
     'Q1097630': 'Intersex', 'Q48270': 'Unknown', 'Q1264807': 'None'}).fillna('Not labeled')
gender_plot = BytesIO()
plt.figure(figsize=(10, 5))
sns.barplot(x=gender_counts.index, y=gender_counts.values, alpha=0.8)
plt.title('Gender Diversity')
plt.ylabel('Number of Occurrences', fontsize=12)
plt.xlabel('Gender', fontsize=12)
plt.savefig(gender_plot, format='png')
gender_plot.seek(0)
gender_plot_encoded = base64.b64encode(gender_plot.getvalue()).decode('utf-8')
plt.close()

# Nationality diversity
nationality_counts = df['nationality'].value_counts(dropna=False).fillna('Not labeled')
nationality_plot = BytesIO()
plt.figure(figsize=(10, 5))
sns.barplot(x=nationality_counts.index, y=nationality_counts.values, alpha=0.8)
plt.title('Nationality Diversity')
plt.ylabel('Number of Occurrences', fontsize=12)
plt.xlabel('Nationality', fontsize=12)
plt.xticks(rotation=90)
plt.savefig(nationality_plot, format='png')
nationality_plot.seek(0)
nationality_plot_encoded = base64.b64encode(nationality_plot.getvalue()).decode('utf-8')
plt.close()

# Year of inception diversity
df['year_of_inception'] = pd.to_datetime(df['year_of_inception'], errors='coerce').dt.year
year_counts = df['year_of_inception'].value_counts(bins=10, dropna=False).rename('Not labeled')
year_plot = BytesIO()
plt.figure(figsize=(10, 5))
sns.histplot(df['year_of_inception'], bins=30)
plt.title('Year of Inception Diversity')
plt.ylabel('Number of Occurrences', fontsize=12)
plt.xlabel('Year of Inception', fontsize=12)
plt.savefig(year_plot, format='png')
year_plot.seek(0)
year_plot_encoded = base64.b64encode(year_plot.getvalue()).decode('utf-8')
plt.close()


@app.route('/')
def index():
    return render_template('index.html',
                           gender_plot=gender_plot_encoded,
                           nationality_plot=nationality_plot_encoded,
                           year_plot=year_plot_encoded)


if __name__ == '__main__':
    app.run(debug=True)