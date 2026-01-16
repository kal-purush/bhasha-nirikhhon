using System;
using System.Collections.Generic;
using System.Text;

namespace JukeBox.Models
{
    public class ArtistRanking
    {
        public int ArtistId { get; set; }
        public string ArtistName { get; set; }
        public long NumberOfVote { get; set; }
        public string ArtistImage { get; set; }
        public long ProgressPosition
        {
            get
            {
                if (NumberOfVote > 0)
                {
                    return NumberOfVote/100;
                }
                return 0;
            }
        }

    }
}