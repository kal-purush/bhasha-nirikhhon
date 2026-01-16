using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KlockanAPI.Application.DTOs.Meeting;

public class MeetingParticipantReportDTO
{
    public string id { get; set; }
    public bool host { get; set; }
    public bool coHost { get; set; }
    public string email { get; set; }
    public string displayName { get; set; }
    public bool invitee { get; set; }
    public bool muted { get; set; }
    public string state { get; set; }
    public DateTime joinedTime { get; set; }
    public DateTime leftTime { get; set; }
    public DateTime meetingStartTime { get; set; }
    public int DurationInMinutes { get; set; }
}