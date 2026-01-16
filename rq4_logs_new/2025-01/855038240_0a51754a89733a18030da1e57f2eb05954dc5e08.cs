using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain.DTOs.Western_MatchDTOs
{
    public class WesternMatchDTO
    {
        public string GameId { get; set; } // CHARACTER VARYING
        public string MatchId { get; set; } // CHARACTER VARYING
        public DateTime DateTimeUtc { get; set; } // TIMESTAMP WITH TIME ZONE
        public string Tournament { get; set; } // CHARACTER VARYING
        public string Team1 { get; set; } // CHARACTER VARYING
        public string Team1Players { get; set; } // CHARACTER VARYING
        public string Team1Picks { get; set; } // CHARACTER VARYING
        public string Team2 { get; set; } // CHARACTER VARYING
        public string Team2Players { get; set; } // CHARACTER VARYING
        public string Team2Picks { get; set; } // CHARACTER VARYING
        public string WinTeam { get; set; } // CHARACTER VARYING
        public string LossTeam { get; set; } // CHARACTER VARYING
        public string Team1Region { get; set; } // CHARACTER VARYING
        public string Team1Longname { get; set; } // CHARACTER VARYING
        public string Team1Short { get; set; } // CHARACTER VARYING
        public List<string> Team1Inputs { get; set; } // TEXT[]
        public string Team2Region { get; set; } // CHARACTER VARYING
        public string Team2Longname { get; set; } // CHARACTER VARYING
        public string Team2Short { get; set; } // CHARACTER VARYING
        public List<string> Team2Inputs { get; set; } // TEXT[]
    }
}