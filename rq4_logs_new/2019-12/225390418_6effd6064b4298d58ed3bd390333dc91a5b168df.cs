using Common;
using System;

namespace DAL
{
    public class AlarmEntity
    {
        public AlarmEntity()
        {
        }

        public AlarmEntity(string serviceId, Alarm alarmDto)
        {
            ServiceId = serviceId;
            TimeOfAlarm = alarmDto.TimeOfAlarm;
            ClientName = alarmDto.NamoOfClient;
            Message = alarmDto.Message;
            Risk = alarmDto.Risk;
        }

        public string ClientName { get; set; }
        public long Id { get; set; }
        public string Message { get; set; }
        public int Risk { get; set; }
        public string ServiceId { get; set; }
        public TimeSpan TimeOfAlarm { get; set; }
    }
}