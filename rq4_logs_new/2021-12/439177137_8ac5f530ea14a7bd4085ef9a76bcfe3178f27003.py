import mwclient
import json
site = mwclient.Site('lol.fandom.com', path='/')

player = 'Thanatos'

ScoreboardPlayers = site.api('cargoquery',
                        limit='max',
                        tables="ScoreboardPlayers",
                        fields="Name,Champion,Kills,Deaths,Assists,Gold,CS,PlayerWin,Role,Side,Items,DamageToChampions, DateTime_UTC",
                        where=f"Name like '{player}' and Role like 'Top'",
                        order_by="DateTime_UTC"
                        )
jsonDict = json.dumps(ScoreboardPlayers, indent=2)
print(jsonDict)