using Google.Apis.Auth.OAuth2;
using Google.Apis.Calendar.v3;
using Google.Apis.Calendar.v3.Data;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace SlccDiscordBot.classes
{
    public class Calendar
    {
        // If modifying these scopes, delete your previously saved credentials
        // at ~/.credentials/calendar-dotnet-quickstart.json
        string[] Scopes = { CalendarService.Scope.CalendarReadonly };
        string ApplicationName = "SLCC Bot";
        CalendarService service = new CalendarService();
        UserCredential credential;
        HashSet<string> calendars = new HashSet<string>();
        List<Events> CombinedEventList = new List<Events>();

        public Calendar()
        {
            using (var stream =
                new FileStream("json\\credentials.json", FileMode.Open, FileAccess.Read))
            {
                string credPath = "token.json";
                credential = GoogleWebAuthorizationBroker.AuthorizeAsync(
                    GoogleClientSecrets.Load(stream).Secrets,
                    Scopes,
                    "user",
                    CancellationToken.None,
                    new FileDataStore(credPath, true)).Result;
                Console.WriteLine("Credential file saved to: " + credPath);
            }

            // Create Google Calendar API service.
            service = new CalendarService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = ApplicationName,
            });

            calendars.Add("bruinmail.slcc.edu_tcet8ho01q49t4laddlcbetpqc@group.calendar.google.com");
            calendars.Add("bruinmail.slcc.edu_jhf8br5bds5fmosrr6ivk3o3to@group.calendar.google.com");
            calendars.Add("bruinmail.slcc.edu_3peeg5obg47g233n28p496uhd8@group.calendar.google.com");
            calendars.Add("bruinmail.slcc.edu_3d3j70osj9jnt2gquuqt64b2es@group.calendar.google.com");
            calendars.Add("en.usa#holiday@group.v.calendar.google.com");
        }

        public string ListAllEvents()
        {
            CombinedEventList = new List<Events>();
            string returnString = string.Empty;
            // List events.
            foreach (string c in calendars)
            {
                CombinedEventList.Add(CreateEventsList(c));
            }
            //Events events = request.Execute();
            returnString += "Upcoming events:\n";
            foreach (Events events in CombinedEventList)
            {
                if (events.Items != null && events.Items.Count > 0)
                {
                    returnString += ($"Calendar Description: {events.Description}\n");
                    foreach (var eventItem in events.Items)
                    {
                        string when = eventItem.Start.DateTime.ToString();
                        if (String.IsNullOrEmpty(when))
                        {
                            when = eventItem.Start.Date;
                        }
                        returnString += ($"\t{eventItem.Summary} ({when})\n");
                    }
                    returnString += ("\n");
                }
            }
            return returnString;
        }

        private Events CreateEventsList(string calendar)
        {
            EventsResource.ListRequest request = service.Events.List(calendar);
            request.TimeMin = DateTime.Now;
            request.ShowDeleted = false;
            request.SingleEvents = true;
            request.MaxResults = 10;
            request.OrderBy = EventsResource.ListRequest.OrderByEnum.StartTime;

            return request.Execute();
        }

    }
}