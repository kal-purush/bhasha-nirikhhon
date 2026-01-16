import pandas as pd
import plotly.figure_factory as ff
import csv

read_csv = pd.read_csv("data.csv")

figure = ff.create_distplot([read_csv["Avg Rating"].tolist()] , ["Height"] )

figure.show()