namespace WolfDen.Application.DTOs.Attendence
{
    public class MonthlyReportDTO
    {
        public int Present { get; set; }
        public int Absent { get; set; }
        public int IncompleteShift { get; set; }
        public int Holiday  { get; set; }
        public List<DateOnly> IncompleteShiftDays { get; set; }
        public List<DateOnly> RestrictedHolidays { get; set; }
        public List<DateOnly> NormalHolidays { get; set; }
        public List<DateOnly> WFHDays { get; set; }
        public List<DateOnly> LeaveDays { get; set; }
        public int Leave { get; set; }
        public List<DateOnly> AbsentDays { get; set; }
        public int WFH{ get; set; }
    }
}