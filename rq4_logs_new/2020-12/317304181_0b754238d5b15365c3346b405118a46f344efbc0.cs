using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventCalendar
{
    public class Event
    {
        //datatime format - dd-mm-yy hh:mm:ss
        public Event()
        {

        }
        public Event(string name, int eventType, DateTime startTime, DateTime endTime)
        {
            Name = name;
            switch (eventType)
            {
                case 1:
                    EventType = EventTypes.Coffee;  break;
                case 2:
                    EventType = EventTypes.Lecture; break;
                case 3:
                    EventType = EventTypes.Concert; break;
                case 4:
                    EventType = EventTypes.StudySession; break;
            }

            StartTime = startTime;
            EndTime = endTime;
        }

        public string Name { get; set; }
        public EventTypes EventType { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }

        public enum EventTypes
        {
            Coffee,
            Lecture,
            Concert,
            StudySession
        }

    }
}