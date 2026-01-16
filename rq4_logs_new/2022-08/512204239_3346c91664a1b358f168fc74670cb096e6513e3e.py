
import pandas as pd

df = pd.read_csv("notas_alunos.csv", sep=";")
media = []
situacao = []
for i in df.index:
    media.append((df['nota_1'][i]+df['nota_2'][i])/2)
    situacao.append("APROVADO" if media[i]>=7 and df['faltas'][i]<=5 else "REPROVADO")
df2 = df.assign(media = media, situacao = situacao)
df2.to_csv("alunos_situacao.csv")

print(f"========TABELA NOTAS_ALUNOS========\n{df}\n\n===============TABELA ALUNOS_SITUACAO===============\n{df2}")

mais_faltas = df2["faltas"].max()
media_geral = df2["media"].median()
maior_media = df2["media"].max()

print(f"\nMAIOR Nº DE FALTAS: {mais_faltas}\nMÉDIA GERAL: {media_geral}\nMAIOR MÉDIA: {maior_media}")