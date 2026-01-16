using System.Collections.Generic;
using System.Linq;
using CounterStats.ApiCaller;

namespace CounterStats.Business.Entities
{
    public class AssaultRifleKillStats
    {
        public long GalilKills { get; }
        public long Ak47Kills { get; }
        public long Sg553Kills { get; }
        public long AwpKills { get; }
        public long G3Kills { get; }
        public long FamasKills { get; }
        public long M4Kills { get; }
        public long Ssg08Kills { get; }
        public long AugKills { get; }
        public long Scar20Kills { get; }

        public long total => GalilKills + Ak47Kills + Sg553Kills + AwpKills + G3Kills + FamasKills + M4Kills +
                             Ssg08Kills + AugKills + Scar20Kills;
        
        internal AssaultRifleKillStats(List<GetUserStatsForGameReturnValue> stats)
        {
            GalilKills = stats.Single(c => c.Name == "total_kills_galilar").Value.ToLong();
            Ak47Kills = stats.Single(c => c.Name == "total_kills_ak47").Value.ToLong();
            Sg553Kills = stats.Single(c => c.Name == "total_kills_sg556").Value.ToLong();
            AwpKills = stats.Single(c => c.Name == "total_kills_awp").Value.ToLong();
            G3Kills = stats.Single(c => c.Name == "total_kills_g3sg1").Value.ToLong();
            FamasKills = stats.Single(c => c.Name == "total_kills_famas").Value.ToLong();
            M4Kills = stats.Single(c => c.Name == "total_kills_m4a1").Value.ToLong();
            Ssg08Kills = stats.Single(c => c.Name == "total_kills_ssg08").Value.ToLong();
            AugKills = stats.Single(c => c.Name == "total_kills_aug").Value.ToLong();
            Scar20Kills = stats.Single(c => c.Name == "total_kills_scar20").Value.ToLong();
        }
    }
}