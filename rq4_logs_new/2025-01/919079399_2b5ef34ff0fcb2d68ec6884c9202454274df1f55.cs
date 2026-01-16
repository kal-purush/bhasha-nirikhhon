using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models.DTOs
{
    public class HistoryResponseDto
    {
        public DateOnly Date { get; set; }

        public HistorySummary Data { get; set; }
    }

    public class HistorySummary
    {
        public float commision { get; set; }
        public float PNL { get; set; }

        public float income { get; set; }

        public float multipler { get; set; }
    }
}