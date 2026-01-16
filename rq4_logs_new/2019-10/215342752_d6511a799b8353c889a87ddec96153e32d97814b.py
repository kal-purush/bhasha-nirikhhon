import pandas as pd
import numpy as np

df = pd.read_csv('wdi_small_tidy_2015.csv')

df.sample(5)

df_GDP = df[['Country Name','GDP per capita (constant 2010 US$)','Mortality rate, infant (per 1,000 live births)']]

df_GDP.sample(5)

from plotnine import *

g = (ggplot(df_GDP, aes(x = 'GDP per capita (constant 2010 US$)', y = 'Mortality rate, infant (per 1,000 live births)')) + 
    geom_point()
    )

print(g)