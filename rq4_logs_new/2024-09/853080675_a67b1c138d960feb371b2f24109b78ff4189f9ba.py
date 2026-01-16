import pandas as pd

## Thanks Mohammad Shahriyar Parvez for filling the missing zip codes
crashes = pd.read_csv("data/cleaned_nyccrashes_msp.csv")

crashes['zip_code'].isnull().sum()

## Save into feather format
crashes.to_feather('data/nyccrashes_cleaned.feather')
