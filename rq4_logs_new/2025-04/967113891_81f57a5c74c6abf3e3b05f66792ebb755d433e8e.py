import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('anxiety_depression_data.csv')

plt.figure(figsize=(10, 6))

# Create bins for social support
df['Social_Support_Bin'] = pd.qcut(df['Social_Support_Score'], 4)

# Melt dataframe for easier plotting
melted = df.melt(id_vars=['Social_Support_Bin'], 
                 value_vars=['Anxiety_Score', 'Depression_Score'],
                 var_name='Condition', value_name='Score')

import seaborn as sns
sns.boxplot(x='Social_Support_Bin', y='Score', hue='Condition', 
            data=melted, palette=['blue', 'red'])
plt.ylabel("Mental Health Score")
plt.xlabel("Social Support Level (Quartiles)")
plt.title("Distribution of Anxiety/Depression by Social Support Level")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()