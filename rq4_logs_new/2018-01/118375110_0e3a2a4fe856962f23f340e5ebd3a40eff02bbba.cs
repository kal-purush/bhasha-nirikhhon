using System.Collections.Generic;
using System.Linq;
using System.Net;
using CounterStats.ApiCaller.Entities;
using Newtonsoft.Json;

namespace CounterStats.ApiCaller
{
    public sealed class SteamApiCaller
    {
        private const string API_KEY = "21F57B48034A17C22F1E49FD0C27D507";

        public GetPlayerSummariesReturnValue GetPlayerSummaries(string steamId)
        {
            var url = $"http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={API_KEY}&steamids={steamId}";

            var jsonString = string.Empty;

            using (WebClient wc = new WebClient())
            {
                jsonString = wc.DownloadString(url);
            }
            var innerResponse = new {players = new List<GetPlayerSummariesReturnValue>()};

            var outerResponse = new {response = innerResponse};
            var returnedObject = JsonConvert.DeserializeAnonymousType(jsonString, outerResponse);

            return returnedObject.response.players.FirstOrDefault();
        } 
    }
}