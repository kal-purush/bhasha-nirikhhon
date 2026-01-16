# Write a script called "chi_squared.py" that loads the data, 
# cleans it, performs the chi-squared test, and prints the result.

import matplotlib.pyplot as plt
import scipy.stats as stats
import pandas as pd
import collections

# Load data on loans and drop null entries
loans = pd.read_csv("https://github.com/Thinkful-Ed/curric-data-001-data-sets/raw/master/loans/loansData.csv")
loans.dropna(inplace=True)

# Run Chi-Squared Test
freq = collections.Counter(loans["Amount.Requested"])
chi, p = stats.chisquare(freq.values())

# Print results
print "Chi: " + str(chi)
print "p: " + str(p)
