import pandas as pd

df_medallas = pd.read_csv("medallas-olimpicas/data/medallas_historicas.csv")
df_continentes = pd.read_csv("medallas-olimpicas/data/pais_continente.csv")

df_merged = pd.merge(df_medallas, df_continentes, on="País", how="left")

print(df_merged[["País", "Continente"]].drop_duplicates())

paises_sin_continente = df_merged[df_merged["Continente"].isna()]["País"].unique()

print("Países sin continente asignado:")
for pais in paises_sin_continente:
    print(pais)