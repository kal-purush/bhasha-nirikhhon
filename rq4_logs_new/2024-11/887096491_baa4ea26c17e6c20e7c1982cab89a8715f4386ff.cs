namespace WolfDen.Application.DTOs.Attendence
{
    public class AttendanceHistoryDTO
    {
        public DateOnly Date { get; private set; }
        public DateTimeOffset? ArrivalTime { get; private set; }
        public DateTimeOffset? DepartureTime { get; private set; }
        public int? InsideDuration { get; private set; }
        public int? OutsideDuration { get; private set; }
        public string? MissedPunch { get; private set; }
        public int AttendanceStatusId { get; set; }
    }
}