using EventPlanner.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace EventPlanner.Services
{
    class EventService
    {
        private readonly string PATH = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"Data\events.json");
        private static EventService singleton = null;


        public static EventService Singleton()
        {
            return singleton ??= new EventService();
        }

        public Event GetEventInfo(int id)
        {
            return GetEvents().FirstOrDefault(el => el.Id == id);
        }

        public List<Event> GetEvents()
        {
            List<Event> events = new List<Event>();
            using (StreamReader reader = new StreamReader(PATH))
            {
                string data = reader.ReadToEnd();
                events = JsonConvert.DeserializeObject<List<Event>>(data);
            }
            return events;
        }

        public List<Event> GetFutureEvents()
        {
            List<Event> events = GetEvents();
            events.RemoveAll(e => e.DateFrom < DateTime.Now);
            return events;
        }

        public List<Event> GetPastEvents()
        {
            List<Event> events = GetEvents();
            events.RemoveAll(e => e.DateFrom > DateTime.Now);
            return events;
        }

        public List<Event> GetEventsForOrganizer(int id)
        {
            List<Event> events = GetEvents();
            events.RemoveAll(e => e.OrganizerId != id);
            return events;
        }

        public List<Event> GetUpcomingEventsForOrganizer(int id)
        {
            List<Event> events = GetEventsForOrganizer(id);
            events.RemoveAll(e => e.DateFrom < DateTime.Now);
            return events;
        }

        public List<Event> GetPastEventsForOrganizer(int id)
        {
            List<Event> events = GetEventsForOrganizer(id);
            events.RemoveAll(e => e.DateFrom > DateTime.Now);
            return events;
        }


        public List<Event> GetFreeEvents()
        {
            List<Event> events = GetEvents();
            events.RemoveAll(e => e.OrganizerId != -1);
            return events;
        }

        public List<Event> GetPotentialEventsForOrganizer(int id)
        {
            List<Event> events = GetFreeEvents();
            List<Event> returning = events.FindAll(e => (e.PotentialOrganizers.Contains(id) || e.PotentialOrganizers.Count == 0));
            return returning;
        }

        public List<Event> Delete(Event e)
        {
            List<Event> events = GetEvents();
            events.RemoveAll(el => el.Id == e.Id);
            save(events);
            return events;
        }

        public List<Event> Modify(Event e)
        {
            List<Event> events = GetEvents();
            for (int i = 0; i < events.Count(); i++)
            {
                if (events[i].Id == e.Id)
                {
                    events[i] = e;
                }
            }
            save(events);
            return events;
        }

        public List<Event> Add(Event e)
        {
            List<Event> events = GetEvents();
            events.Add(e);
            save(events);
            return events;
        }

        public int GetLastId()
        {
            List<Event> events = GetEvents();
            int greatest = 1;
            foreach (var e in events)
            {
                if (greatest < e.Id)
                {
                    greatest = e.Id;
                }
            }
            return ++greatest;
        }

        public void save(List<Event> events)
        {
            using (StreamWriter writer = new StreamWriter(PATH))
            {
                string data = JsonConvert.SerializeObject(events);
                writer.WriteLine(data);
            }
        }
    }
}