using System.Collections.Generic;

namespace BlizzardAPI.SC2.Profile
{
    public class Ladders
    {
        public List<CurrentSeason> CurrentSeason { get; set; }
        public List<PreviousSeason> PreviousSeason { get; set; }
        public List<object> ShowcasePlacement { get; set; }

        public Ladders(string accountId, string name, string region, string profileRegion, string locale)
        {
            var laddersData = ApiHelper.GetJsonFromUrl(
                $"https://{region}.api.battle.net/sc2/profile/{accountId}/{profileRegion}/{name}/ladders?locale={locale}&apikey={ApiHandler.ApiKey}"
            );

            if (laddersData == null)
                return;

            CurrentSeason = laddersData["currentSeason"].ToObject<List<CurrentSeason>>();
            PreviousSeason = laddersData["previousSeason"].ToObject< List<PreviousSeason>>();
            ShowcasePlacement = laddersData["showcasePlacement"].ToObject<List<object>>();
        }
    }
}