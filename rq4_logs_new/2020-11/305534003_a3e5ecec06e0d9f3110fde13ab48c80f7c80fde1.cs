using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;
using QuickType;

namespace GroupProject.Pages
{
    public class IndexModel : PageModel
    {
        private readonly ILogger<IndexModel> _logger;

        public IndexModel(ILogger<IndexModel> logger)
        {
            _logger = logger;
        }

        public void OnGet()
        {
            using(var webClient = new WebClient())
            {
                //IDictionary<long, QuickTypePlayerDetails.PlayerDetail> allplayers = new Dictionary<long, QuickTypePlayerDetails.PlayerDetail>();
                //String playersJSON = webClient.DownloadString("https://api.sportsdata.io/v3/soccer/scores/json/CompetitionDetails/EPL?key=bc49021bad1943008414c5a75e665961");
                //QuickTypePlayerDetails.PlayerDetail[] playerDetails = QuickTypePlayerDetails.PlayerDetail.FromJson(playersJSON);
                

                //foreach (QuickTypePlayerDetails.PlayerDetail player in playerDetails)
                //{
                 //   allplayers.Add(player.PlayerId, player);
                //}

                string jsonString = webClient.DownloadString("https://api.sportsdata.io/v3/soccer/scores/json/MembershipsByCompetition/EPL?key=bc49021bad1943008414c5a75e665961");
                JSchema schema = JSchema.Parse(System.IO.File.ReadAllText("PlayerInfoSchema.json"));
                JArray jsonObject = JArray.Parse(jsonString);
                IList<string> validationEvents = new List<string>();
                if (jsonObject.IsValid(schema, out validationEvents))
                {
                    var playerinfo = PlayerInfo.FromJson(jsonString);
                   
                  
                    ViewData["PlayerInfo"] = playerinfo;
                    
                }
                else
                {
                    foreach(string evt in validationEvents)
                    {
                        Console.WriteLine(evt);

                    }
                    ViewData["PlayerInfo"] = new List<PlayerInfo>();
                }
                
            }

        }
    }
}