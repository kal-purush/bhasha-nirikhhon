using System.Collections.Generic;
using System.Linq;
using CounterStats.ApiCaller;

namespace CounterStats.Business.Entities
{
    public class PistolKillStats
    {
        public long Glock18Kills { get; }
        public long P250Kills { get; }
        public long DesertEagleKills { get; }
        public long DualBerettasKills { get; }
        public long Tec9Kills { get; }
        public long FiveSevenKills { get; }
        public long P2000Kills { get; }

        public long Total => Glock18Kills + P250Kills + DesertEagleKills + DualBerettasKills + Tec9Kills + FiveSevenKills +
                             P2000Kills;

        internal PistolKillStats(List<GetUserStatsForGameReturnValue> stats)
        {
            Glock18Kills = stats.Single(c => c.Name == "total_kills_glock").Value.ToLong();
            P250Kills = stats.Single(c => c.Name == "total_kills_p250").Value.ToLong();
            DesertEagleKills = stats.Single(c => c.Name == "total_kills_deagle").Value.ToLong();
            DualBerettasKills = stats.Single(c => c.Name == "total_kills_elite").Value.ToLong();
            Tec9Kills = stats.Single(c => c.Name == "total_kills_tec9").Value.ToLong();
            FiveSevenKills = stats.Single(c => c.Name == "total_kills_fiveseven").Value.ToLong();
            P2000Kills = stats.Single(c => c.Name == "total_kills_hkp2000").Value.ToLong();
        }

    }
}