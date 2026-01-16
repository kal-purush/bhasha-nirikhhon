using System.Collections.Generic;
using System.Linq;
using CounterStats.ApiCaller;

namespace CounterStats.Business.Entities
{
    public class ShotgunGunKills
    {
        public long SawedOffKills { get; }
        public long NovaKills { get; }
        public long Xm1014Kills { get; }
        public long Mag7Kills { get; }

        public long Total => SawedOffKills + NovaKills + Xm1014Kills + Mag7Kills;

        internal ShotgunGunKills(List<GetUserStatsForGameReturnValue> stats)
        {
            SawedOffKills = stats.Single(c => c.Name == "total_kills_sawedoff").Value.ToLong();
            NovaKills = stats.Single(c => c.Name == "total_kills_nova").Value.ToLong();
            Xm1014Kills = stats.Single(c => c.Name == "total_kills_xm1014").Value.ToLong();
            Mag7Kills = stats.Single(c => c.Name == "total_kills_mag7").Value.ToLong();
        }
    }
}