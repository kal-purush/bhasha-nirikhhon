using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZestAPI.Models
{
    public class Member
    {
        [JsonProperty("id")]
        public int ID { get; set; }
        [JsonProperty("username")]
        public string Name{get;set;}
        [JsonProperty("email")]
        public string Email { get; set; }
        [JsonProperty("is_staff")]
        public bool IsStaff { get; set; }
        [JsonProperty("is_active")]
        public bool IsActive { get; set; }
        [JsonProperty("site")]
        public string Site { get; set; }
        [JsonProperty("avatar_url")]
        public string AvatarUrl { get; set; }
        [JsonProperty("biography")]
        public string Biography { get; set; }
        [JsonProperty("sign")]
        public string Signature { get; set; }
        [JsonProperty("email_for_answer")]
        public bool AllowMailForAnswer { get; set; }
        [JsonProperty("last_visit")]
        public DateTime LastVisiteDate { get; set; }
    }
}