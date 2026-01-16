import requests
import bs4
import re
import pandas as pd

all_players = pd.DataFrame()
for year in range(1980, 2021):
    print(f'Pulling data from {year}...')
    page_url = f'https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html'
    text = requests.get(page_url).text
    table = bs4.BeautifulSoup(text, 'html.parser').find('tbody')
    rows = table.find_all('tr', attrs={'class': 'full_table'})
    player_stats = [i.find_all('td') for i in rows]

    for k, p in enumerate(player_stats):
        # Get the column names for the table
        cols = ['year'] + [re.findall('(?<=data-stat=")[\\d\\w\\.]+', str(i))[0]
                for i in player_stats[k]]
        # Get the stat values for each column
        vals = [year] + \
            re.findall('(?<=append-csv=")[-\\s\\d\\w\\.]+',
                       str(player_stats[k][0])) + \
            [['0.0'][0] if re.findall('(?<=>)[-\\d\\w\\.]+(?=<)', str(i)) == []
             else re.findall('(?<=>)[-\\d\\w\\.]+(?=<)', str(i))[0]
             for i in player_stats[k][1:len(player_stats[k])]]
        # Make it into a dataframe
        df_row = pd.DataFrame(vals, index=cols).transpose()
        all_players = pd.concat([all_players, df_row])

all_players.to_csv('~/Desktop/player_stats.csv')