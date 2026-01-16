using System;
using System.Threading.Tasks;

namespace TftTracker.Api
{
    public class SummonerV1Processor
    {
        public async Task LoadSummoner(string summonerName)
        {
            if (String.IsNullOrWhiteSpace(summonerName))
                return;

            string url = $"oc1.api.riotgames.com/tft/summoner/v1/summoners/by-name/{summonerName}"; //Will need logic for grabbing the region
            using (HttpClient httpClient = new HttpClient())
            using (HttpResponseMessage response = await httpClient.GetAsync(url))
            {
                //Fun stuff goes here
            }
        }

        //Other endpoints for loading summoner can overload LoadSummoner() if/when I implement them
    }
}