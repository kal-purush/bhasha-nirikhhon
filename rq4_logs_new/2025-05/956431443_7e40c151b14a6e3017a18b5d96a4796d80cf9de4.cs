using System.Collections;
using CSL.SubTypes;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;

namespace CSL.Tests;

public class GeneratorTests
{
    public static IEnumerable GeneratorTestCases
    {
        get
        {
            var ical = new Ical.Net.Calendar();
            var boosterEvent = new CalendarEvent
            {
                // If Name property is used, it MUST be RFC 5545 compliant
                Summary = "Bund en booster",
                Start = new CalDateTime(new DateTime(2025, 6, 25, 17, 50, 0)),
                End = new CalDateTime(new DateTime(2025, 6, 25, 17, 55, 0)),
            };
        
            var hygiejneEvent = new CalendarEvent
            {
                // If Name property is used, it MUST be RFC 5545 compliant
                Summary = "Vask hænder",
                Start = new CalDateTime(new DateTime(2025, 6, 25, 17, 55, 0)),
                End = new CalDateTime(new DateTime(2025, 6, 25, 18, 0, 0)),
            };
        
            var aftensmadEvent = new CalendarEvent
            {
                // If Name property is used, it MUST be RFC 5545 compliant
                Summary = "Aftensmad",
                Start = new CalDateTime(new DateTime(2025, 6, 25, 18, 0, 0)),
                End = new CalDateTime(new DateTime(2025, 6, 25, 19, 0, 0)),
            };

            ical.Events.Add(boosterEvent);
            ical.Events.Add(hygiejneEvent);
            ical.Events.Add(aftensmadEvent);
            
            yield return new TestCaseData(new Calendar([
                new Event(Subject: "Bund en booster", Date: new Date(25, 6, 2025), Clock: new Clock(17, 50), Duration: Duration.FromMinutes(5)),
                new Event(Subject: "Vask hænder", Date: new Date(25, 6, 2025), new Clock(17, 55), Duration: Duration.FromMinutes(5)),
                new Event(Subject: "Aftensmad", new Date(25, 6, 2025), new Clock(18, 0), Duration: Duration.FromHours(1))
            ]), ical);
        }
    }
    
    [TestCaseSource(nameof(GeneratorTestCases))]
    public void CompareCalendars(Calendar calendar, Ical.Net.Calendar expectedCalendar)
    {
        
    }
}