import pandas as pd


df = pd.read_csv('bereinigt.csv')

min_mandate = 88

# § 3
# Wahl der Abgeordneten nach den Landeslisten
#
# (1) Bei der Verteilung der Sitze auf die Landeslisten werden nur Parteien, politische Vereinigungen und
# Listenvereinigungen berücksichtigt, die mindestens fünf vom Hundert der im Wahlgebiet abgegebenen gültigen
# Zweitstimmen erhalten oder mindestens in einem Wahlkreis einen Sitz errungen haben.
n_stimmen = df['n_listenstimmen'].sum()
df['5%_schwelle'] = False
df.loc[df['n_listenstimmen'] >= n_stimmen * 0.05, '5%_schwelle'] = True
# oder in mindestens einem Wahlkreisen ein Direktmandat errungen haben.
df.loc[df['n_direkt_mandate'] >= 1, '5%_schwelle'] = True

# Von der Gesamtzahl der nach § 1 Absatz 1 Satz 1 zu wählenden Abgeordneten wird die Zahl der erfolgreichen
# Wahlkreisbewerbenden abgezogen, die in Satz 2 genannt sind.
min_mandate -= df.loc[df['5%_schwelle'] == False, 'n_direkt_mandate'].sum()

# (3) Die nach Absatz 2 Satz 3 verbleibenden Sitze werden auf die Landeslisten auf der Grundlage der zu
# berücksichtigenden Zweitstimmen verteilt. Dabei wird die Gesamtzahl der verbleibenden Sitze mit der Zahl der
# Zweitstimmen vervielfacht, die eine Landesliste erhalten hat, und durch die Gesamtzahl der Zweitstimmen aller zu
# berücksichtigenden Landeslisten geteilt.
df_5_prozent = df.loc[df['5%_schwelle'], ['partei', 'n_listenstimmen']]
df_5_prozent['bruchteile'] = df_5_prozent['n_listenstimmen'] * min_mandate / df_5_prozent['n_listenstimmen'].sum()

# Jede Landesliste erhält zunächst so viele Sitze, wie ganze Zahlen auf sie entfallen.
df_5_prozent['ganze'] = df_5_prozent.bruchteile.astype(int)

# Die restlichen zu vergebenden Sitze sind den Landeslisten in der Reihenfolge der höchsten Zahlenbruchteile, die
# sich bei der Berechnung nach Satz 2 ergeben, zuzuteilen.
df_5_prozent['bruchteile'] -= df_5_prozent['ganze']
df_5_prozent = df_5_prozent.sort_values('bruchteile', ascending=False)
zu_vergeben = min_mandate - df_5_prozent['ganze'].sum()
df_5_prozent.iloc[:zu_vergeben]['ganze'] += 1

df['n_gesamtmandate'] = pd.merge(df, df_5_prozent, on='partei', how='left')['ganze'].fillna(0).astype(int)

# Ueberhangmandate
if any(df['n_direkt_mandate'] > df['n_gesamtmandate']):
    raise ValueError('Überhangmandate')

df.to_csv('Ergebnis.csv', index=False)