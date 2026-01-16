using System.Collections.Generic;
using System.Linq;
using CounterStats.ApiCaller;

namespace CounterStats.Business.Entities
{
    public class SubmachineGunKills
    {
        public long Mac10Kills { get; }
        public long Mp7Kills { get; }
        public long Ump45Kills { get; }
        public long BizonKills { get; }
        public long P90Kills { get; }
        public long Mp9Kills { get; }

        public long Total => Mac10Kills + Mp7Kills + Ump45Kills + BizonKills + P90Kills + Mp9Kills;

        internal SubmachineGunKills(List<GetUserStatsForGameReturnValue> stats)
        {
            Mac10Kills = stats.Single(c => c.Name == "total_kills_mac10").Value.ToLong();
            Mp7Kills = stats.Single(c => c.Name == "total_kills_mp7").Value.ToLong();
            Ump45Kills = stats.Single(c => c.Name == "total_kills_ump45").Value.ToLong();
            BizonKills = stats.Single(c => c.Name == "total_kills_bizon").Value.ToLong();
            P90Kills = stats.Single(c => c.Name == "total_kills_p90").Value.ToLong();
            Mp9Kills = stats.Single(c => c.Name == "total_kills_mp9").Value.ToLong();
        }
    }
}