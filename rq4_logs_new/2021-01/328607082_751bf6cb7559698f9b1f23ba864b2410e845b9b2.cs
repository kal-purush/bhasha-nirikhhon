using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BakalariClient.Models
{
    class Config
    {
        [JsonProperty(PropertyName = "credentials", Required = Required.Always)]
        public Credential Credential { get; set; }
        [JsonProperty(PropertyName = "domain", Required = Required.Always)]
        public string Domain
        {
            get => _Domain;
            set
            {
                if (value.EndsWith("/login"))
                {
                    _Domain = value.Substring(value.Length - 6);
                }
                else
                {
                    _Domain = value;
                }
            }
        }

        [JsonIgnore]
        public string _Domain { get; set; }

        [JsonIgnore]
        public string LoginUrl
        {
            get => _Domain + "/login";
        }

        [JsonIgnore]
        public string ScheduleUrl
        {
            get => _Domain + "/next/rozvrh.aspx";
        }
    }
}