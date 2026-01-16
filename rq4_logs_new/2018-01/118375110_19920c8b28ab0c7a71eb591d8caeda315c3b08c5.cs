using System.Collections.Generic;
using System.Linq;
using CounterStats.ApiCaller;

namespace CounterStats.Business.Entities
{
    public class CloseQuartersKills
    {
        public long KnifeKills { get; }
        public long ZeusKills { get; }

        public long Total => KnifeKills + ZeusKills;

        internal CloseQuartersKills(List<GetUserStatsForGameReturnValue> stats)
        {
            KnifeKills = stats.Single(c => c.Name == "total_kills_knife").Value.ToLong();
            ZeusKills = stats.Single(c => c.Name == "total_kills_taser").Value.ToLong();
        }
    }
}