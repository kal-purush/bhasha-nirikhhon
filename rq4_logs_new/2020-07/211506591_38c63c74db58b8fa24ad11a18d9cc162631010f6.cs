using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DTOs
{
    public class TibiaCharacterQuery
    {
        [JsonProperty("characters")]
        public Characters Characters { get; set; }
    }

    public class Characters
    {
        [JsonProperty("data")]
        public Data Data { get; set; }
    }

    public class Data
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("guild")]
        public Guild Guild { get; set; }

        [JsonProperty("vocation")]
        public string Vocation { get; set; }

        [JsonProperty("level")]
        public int Level { get; set; }

        [JsonProperty("world")]
        public string World { get; set; }
    }

    public class Guild
    {
        [JsonProperty("name")]
        public string Name { get; set; }
    }
}