using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace CK3.Utils.BattleSimulator.Data
{
    public class RegimentBonuses
    {
        [JsonProperty("damage")]
        public int Damage { get; set; }

        [JsonProperty("toughness")]
        public int Toughness { get; set; }

        [JsonProperty("pursuit")]
        public int Pursuit { get; set; }

        [JsonProperty("screen")]
        public int Screen { get; set; }

        [JsonProperty("damage_multiplier")]
        public double DamageMultiplier { get; set; } = 1.0;

        [JsonProperty("toughness_multiplier")]
        public double ToughnessMultiplier { get; set; } = 1.0;

        [JsonProperty("pursuit_multiplier")]
        public double PursuitMultiplier { get; set; } = 1.0;

        [JsonProperty("screen_multiplier")]
        public double ScreenMultiplier { get; set; } = 1.0;
    }
}