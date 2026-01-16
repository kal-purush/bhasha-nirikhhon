using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WolfDen.Application.Requests.DTOs.Attendence
{
    public class WeeklySummaryDTO
    {
        public DateOnly Date { get;  set; }
        public DateTime ArrivalTime { get;  set; }
        public DateTime DepartureTime { get;  set; }
        public int InsideDuration { get;  set; }
        public int OutsideDuration { get;  set; }
        public string MissedPunch { get;  set; }
        public string Status { get; set; }
    }
}