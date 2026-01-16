import pandas as pd

#Convert the results in the .tsv file to a JSON file
df = pd.read_csv("npsurvey-2020-data/numpy_survey_results.tsv", sep='\t')

df.to_json("data.json", orient="records", indent=4)