using System.Collections.Generic;

namespace BlizzardAPI.SC2.Ladder
{
    public class Ladder
    {
        public List<LadderMember> LadderMembers { get; set; }

        public Ladder(string ladderId, string region, string locale)
        {
            var ladderData = ApiHelper.GetJsonFromUrl(
               $"https://{region}.api.battle.net/sc2/ladder/{ladderId}?locale={locale}&apikey={ApiHandler.ApiKey}"
           );

            if (ladderData == null)
                return;

            LadderMembers = ladderData["ladderMembers"].ToObject<List<LadderMember>>();
        }
    }
}