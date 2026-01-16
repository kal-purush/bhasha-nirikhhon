using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BL.MailTemplates
{
    public class MailTemplateInviteTeamscan
    {
        public string TemplateId { get; } = "d-21c54333f41540d191c1f67f3ecfa998";

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("teamname")]
        public string TeamName { get; set; }

        [JsonProperty("teamleaderName")]
        public string TeamleaderName { get; set; }

        [JsonProperty("url")]
        public string Url { get; set; }
    }
}