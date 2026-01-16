import pandas as pd
df = pd.read_csv('TT.txt', quotechar='"', skipinitialspace = True)
print(df)


df = df[(df['mode'] != 'Seed') & (df['mode'] != 'Grounds')]
df['Timestamp'] = pd.to_datetime(df['datetime_utc'])
df.set_index('Timestamp')

print(df)