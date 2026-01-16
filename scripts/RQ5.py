import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter
sns.set_palette("Set2")
# Load the data
df = pd.read_csv('repo_language_classification1.csv')

# Count number of repos per classification
class_counts = df['classification'].value_counts().reset_index()
class_counts.columns = ['classification', 'count']

# Plot with seaborn
plt.figure(figsize=(10, 4))
sns.barplot(data=class_counts, x='classification', y='count')
# plt.yscale("log")
plt.ylabel("Number of \nRepositories", fontsize=18)
plt.xlabel(None)
# plt.title("Number of Repos by Language Classification")
plt.xticks(rotation=90, fontsize=18)
plt.yticks(fontsize=18)
def millions(x, pos):
    return f'{x/1e6:.1f}M' if x >= 1e6 else (f'{x/1e3:.0f}k' if x >= 1e3 else str(int(x)))
plt.gca().yaxis.set_major_formatter(FuncFormatter(millions))
plt.tight_layout()
plt.savefig("RQ5.pdf", bbox_inches="tight")
plt.show()