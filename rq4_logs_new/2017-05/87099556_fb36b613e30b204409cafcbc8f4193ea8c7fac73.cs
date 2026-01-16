using System.Collections.Generic;

namespace BlizzardAPI.SC2.Profile
{
    public class MatchHistory
    {
        public List<Match> Matches { get; set; }

        public MatchHistory(string accountId, string name, string region, string profileRegion, string locale)
        {
            var matchHistoryData = ApiHelper.GetJsonFromUrl(
               $"https://{region}.api.battle.net/sc2/profile/{accountId}/{profileRegion}/{name}/matches?locale={locale}&apikey={ApiHandler.ApiKey}"
           );

            if (matchHistoryData == null)
                return;

            Matches = matchHistoryData["matches"].ToObject<List<Match>>();
        }
    }
}