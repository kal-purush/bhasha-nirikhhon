using System.Collections.Generic;
using System.Linq;
using CounterStats.ApiCaller;

namespace CounterStats.Business.Entities
{
    public class LightMachinegunKills
    {
        public long M249Kills { get; }
        public long NegevKills { get; }

        public long Total => M249Kills + NegevKills;

        internal LightMachinegunKills(List<GetUserStatsForGameReturnValue> stats)
        {
            M249Kills = stats.Single(c => c.Name == "total_kills_m249").Value.ToLong();
            NegevKills = stats.Single(c => c.Name == "total_kills_negev").Value.ToLong();
        }
    }
}