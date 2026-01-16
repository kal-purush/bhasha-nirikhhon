using System.Collections.Generic;
using Newtonsoft.Json;

namespace DFC.App.MatchSkills.Services.ServiceTaxonomy.Models
{
    public class GetOccupationsWithMatchingSkillsRequest
    {
        [JsonProperty("minimumMatchingSkills")]
        public int MinimumMatchingSkills { get; set; }


        [JsonProperty("skillList")]
        public IList<string> SkillList { get; set; }

        public GetOccupationsWithMatchingSkillsRequest()
        {
            SkillList = new List<string>();
        }
    }
}