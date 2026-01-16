import urllib, json, psycopg2,time
#Enter database Information
dbname = "riotdb"
user = "dbuser"
password = "dbpass"
table = "challengers"

#Pull from challenger list, which has 200 players
playerListUrl = "https://na.api.pvp.net/api/lol/na/v2.5/league/challenger?type=RANKED_SOLO_5x5&api_key=90b9a55c-a201-4c04-82f3-daa7e4faf715"
listResponse = urllib.urlopen(playerListUrl)
#playerListData holds our JSON. 1 call for all 200 player Ids
playerListData = json.loads(listResponse.read())

try:
    conn = psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")
    print "Successfully connected!"
except:
    print "Failed to connect to database. Check authentication."

cur = conn.cursor() #cursor can execute SQL commands. If connection fails, conn is not defined!

cur.execute("CREATE TABLE IF NOT EXISTS " + table + "(tablekey FLOAT, playerId INT, playerRank VARCHAR, championId INT, championPoints INT, championLevel INT);")

#Rate limiting logic, uses time library
def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate

@RateLimited(0.8)  #  About one call every 1.2 seconds at most
def rateLimitUrl(currUrl): 
    urlResponse = urllib.urlopen(currUrl)
    urlData = json.loads(urlResponse.read())
    return urlData

if __name__ == "__main__":
    listlength = len(playerListData["entries"])
    for i in range(1, listlength):
        playerId = playerListData["entries"][i]["playerOrTeamId"]
        masteryUrl = "https://na.api.pvp.net/championmastery/location/NA1/player/" + playerId + "/champions?api_key=90b9a55c-a201-4c04-82f3-daa7e4faf715"
        masteryData = rateLimitUrl(masteryUrl)
        masteryLength = len(masteryData)
        for j in range(1, masteryLength):
            tablekey = str(masteryData[j]["playerId"]) + str(j) + str(masteryData[j]["championPoints"])
            playerId = str(masteryData[j]["playerId"])
            playerRank = str(playerListData["tier"]) 
            championId = str(masteryData[j]["championId"])
            championPoints = str(masteryData[j]["championPoints"])
            championLevel = str(masteryData[j]["championLevel"])
            try:  
                cur.execute("INSERT INTO " + table +" VALUES (" + tablekey + ", " + playerId + ", '" + playerRank + "', " + championId + ", " + championPoints + ", " + championLevel + ");")
                print "Row added for: " + str(playerId)
                conn.commit()
            except Exception,e:
                print str(e)