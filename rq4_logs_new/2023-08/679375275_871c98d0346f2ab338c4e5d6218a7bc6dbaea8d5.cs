using System;
namespace MODELS
{
    public enum confirmation
    {
        No_Action,
        Not_Going,
        Confirmed
    }
    public class ContactClass
    {
        public string? email { get; set; }
        public string? fn { get; set; }
        public string? ln { get; set; }
        public string? note { get; set; }
    }

    public class MeetingConfirmation
    {
        public string? id { get; set; }
        public int code { get; set; }
        public bool confirmation { get; set; }
        public string? date { get; set; }
        public DateTime? added { get; set; }
        public DateTime? updated { get; set; }
        public string? email { get; set; }
    }
    public enum MeetingFrequency {
        OneTime,
        Daily,
        Weekly,
        BiWeekly,
        Monthly,
        Yearly
    }

    public class MeetingTime
    {
        public string? id { get; set; }
        public string? host { get; set; }
        public MeetingFrequency? frequency { get; set; }
        public string? timezone { get; set; }
        public string? day { get; set; }
        public string? time { get; set; }
        public string? url { get; set; }
        public DateTime? added { get; set; }
        public DateTime? updated { get; set; }
        public string? creator { get; set; }
    }
}
