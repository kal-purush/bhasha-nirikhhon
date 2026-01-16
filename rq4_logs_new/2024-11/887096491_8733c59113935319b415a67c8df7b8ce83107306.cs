using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WolfDen.Domain.Enums;

namespace WolfDen.Application.DTOs.Attendence
{
    public class WeeklySummaryDTO
    {
        public DateOnly Date { get; set; }
        public DateTime ArrivalTime { get; set; }
        public DateTime DepartureTime { get; set; }
        public int InsideDuration { get; set; }
        public int OutsideDuration { get; set; }
        public string? MissedPunch { get; set; }
        public AttendanceStatus AttendanceStatusId { get; set; }
    }
}