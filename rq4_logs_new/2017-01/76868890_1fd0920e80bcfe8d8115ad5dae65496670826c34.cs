using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnePiece.App.Models
{
    public class Season
    {
        public string Title { get; set; }
        public string Url { get; set; }
        public string Thumbnail { get; set; }
        public string Description { get; set; }
        public int LatestEpisode { get; set; }
        public int TotalEpisodes { get; set; }

        public string EpisodeInfo => LatestEpisode + "/" + TotalEpisodes;
    }
}