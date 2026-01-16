using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace YoTennis.Models
{
    public class PlayerStatsModel
    {        
        public string Player { get; set; }
        public int Matches { get; set; }
        public int Completed { get; set; }
        public int Won { get; set; }
        public int Lost { get; set; }
        public PlayerStatsMatchModel AggregatedMatchStats { get; set; }

        public static PlayerStatsModel operator +(PlayerStatsModel left, PlayerStatsModel right) => new PlayerStatsModel
        {
            Player = left.Player != null ? left.Player : right.Player,
            Matches = left.Matches + right.Matches,
            Completed = left.Completed + right.Completed,
            Won = left.Won + right.Won,
            Lost = left.Lost + right.Lost,
            AggregatedMatchStats = left.AggregatedMatchStats + right.AggregatedMatchStats
        };
    }
}