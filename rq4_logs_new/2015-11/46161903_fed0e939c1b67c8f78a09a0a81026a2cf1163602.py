from __future__ import division   # This imports division which allows us to see decimals for statistics
import requests                   # Imports requests, which is used in MatchAPI
import MatchConsts as Consts      # Imports our constants, a .py file in the same directory, which details the URL to get, versions, and regions.

class MatchAPI(object):

    def __init__(self, api_key, region=Consts.REGIONS['north_america']):
        self.api_key = api_key
        self.region = region

    def _request(self, api_url, params={}):
        args = {'api_key': self.api_key}
        for key, value in params.items():
            if key not in args:
                args[key] = value
        response = requests.get(
            Consts.URL['base'].format(
                proxy=self.region,
                region=self.region,
                url=api_url
                ),
            params=args
            )
        print response.url
        return response.json()

    def get_match_by_Id(self, id):
        api_url = Consts.URL['match_by_Id'].format(
            version=Consts.API_VERSIONS['match'],
            matchId=id
            )
        return self._request(api_url)

matchID = 2013680142

def main():
    api = MatchAPI('493ee8ca-0831-418b-bcc8-e1ddd7e582b8')
    stats = api.get_match_by_Id(matchID)
    print stats

# The next two lines start the code
if __name__ == '__main__':
    main()


""" kills = stats ['games'][0]['stats']['championsKilled']
    deaths = stats ['games'][0]['stats']['numDeaths']
    assists = stats ['games'][0]['stats']['assists']
    KDA = (kills+assists)/deaths
    gold = stats ['games'][0]['stats']['goldEarned']
    minions = stats ['games'][0]['stats']['minionsKilled']
    time = stats ['games'][0]['stats']['timePlayed']
    heal = stats ['games'][0]['stats']['totalHeal']
    unitshealed = stats ['games'][0]['stats']['totalUnitsHealed']
    damagedone = stats ['games'][0]['stats']['totalDamageDealtToChampions']
    cc = stats ['games'][0]['stats']['totalTimeCrowdControlDealt']
    physcialtaken = stats ['games'][0]['stats']['physicalDamageTaken']
    magictaken = stats ['games'][0]['stats']['magicDamageTaken']
    print KDA
    print (gold/time)*60
    print minions/(time/60)
    print heal
    print damagedone
    print physcialtaken+magictaken """

minutes = 1754
seconds = minutes//100 *60 + minutes %100
time = seconds/60
print time

