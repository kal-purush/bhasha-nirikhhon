using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace YoTennis.Models
{
    public class MatchInfoModel
    {
        public string MatchId { get; set; }
        public string FirstPlayer { get; set; }
        public string SecondPlayer { get; set; }
        public DateTime MatchStartedAt { get; set; }
        public MatchState State { get; set; }
        public string MatchScore { get; set; }
        public Player? Winner { get; set; }
    }
}