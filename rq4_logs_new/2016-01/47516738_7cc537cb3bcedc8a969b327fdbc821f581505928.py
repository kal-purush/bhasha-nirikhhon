from json import load
from unidecode import unidecode
j= load( open("database/songs.json") )
def unis(s):
    if type(s)==unicode:
        return unidecode(s)
    return str(s)

with open("database/songs.csv","w") as out:
    print >> out, ";".join(j[0].keys())
    out.writelines( [ ";".join( [ unis(x) for x in r.values()] ) for r in j ] )

