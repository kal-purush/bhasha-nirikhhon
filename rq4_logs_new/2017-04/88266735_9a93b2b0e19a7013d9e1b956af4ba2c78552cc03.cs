using Google.Apis.Auth.OAuth2;
using Google.Apis.Calendar.v3;
using Google.Apis.Calendar.v3.Data;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cal_App.Asset
{
    class GoogleCal
    {
        // If modifying these scopes, delete your previously saved credentials
        // at ~/.credentials/calendar-dotnet-quickstart.json
        private string[] Scopes = { CalendarService.Scope.CalendarReadonly };
        private string ApplicationName = "Google Calendar API .NET Quickstart";


        public string GetEvents(int year, int month, int day)
        {
            UserCredential credential;

            using (var stream =
              new FileStream("client_secret.json", FileMode.Open, FileAccess.Read))
            {
                string credPath = System.Environment.GetFolderPath(
                  System.Environment.SpecialFolder.Personal);
                credPath = Path.Combine(credPath, ".credentials/calendar-dotnet-quickstart.json");

                credential = GoogleWebAuthorizationBroker.AuthorizeAsync(
                  GoogleClientSecrets.Load(stream).Secrets,
                  Scopes,
                  "user",
                  CancellationToken.None,
                  new FileDataStore(credPath, true)).Result;
                Console.WriteLine("Credential file saved to: " + credPath);
            }

            // Create Google Calendar API service.
            var service = new CalendarService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = ApplicationName,
            });

            // Define parameters of request.
            EventsResource.ListRequest request = service.Events.List("primary");
            DateTime questionedDay = new DateTime(year, month, day);
            request.TimeMin = questionedDay;
            request.ShowDeleted = false;
            request.SingleEvents = true;
            TimeSpan oneDay = new TimeSpan(1, 0, 0, 0);
            request.TimeMax = questionedDay + oneDay;
            //request.MaxResults = 15;
            request.OrderBy = EventsResource.ListRequest.OrderByEnum.StartTime;

            // List events.
            var result = String.Empty;
            Events events = request.Execute();
            if (events.Items != null && events.Items.Count > 0)
            {
                foreach (var eventItem in events.Items)
                {
                    var when = eventItem.Start.DateTime;
                    result += String.Format("{0:H:mm} - {1}\n", when, eventItem.Summary);
                }
            }

            return result;
        }
    }
}