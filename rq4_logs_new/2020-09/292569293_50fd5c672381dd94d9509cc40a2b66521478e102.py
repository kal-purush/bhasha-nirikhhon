"""Histogram plot script
"""

# author: Durgesh Samariya
# github: themlphdstudent
# Licence: BSD 3-Clause

#import libraries
import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv('../data/heart_disease.csv')

plt.hist(data['age'])
plt.xlabel('Age')
plt.ylabel('Count')
plt.title('Age distribution.')
plt.show()
plt.savefig('../figures/Histogram_Plot.png', dpi=300)