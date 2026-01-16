using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace homework
{
    class Program
    {
        static void Main(string[] args)
        {
            EventService service = new EventService();

            foreach (var item in service.GetEvents())
            {
                Console.WriteLine(item);
                Console.WriteLine();
            }
            Console.ReadLine();
        }
    }
    public class Event
    {
        public string Name { get; set; }
        public string Location { get; set; }
        public DateTime StartTime { get; set; }

        public override string ToString()
        {
            string output = "";
            output += string.Format("Name: {0}\nLocation: {1}\nTime:{2:f}", this.Name, this.Location, this.StartTime);
            return output;
        }
    }

    public class EventService
    {
        public virtual IEnumerable<Event> GetEvents()
        {
            return new List<Event>
           {
               new Event { Name ="My event 1", Location = "Here", StartTime = DateTime.Now},
               new Event { Name ="My event 2", Location = "There", StartTime = DateTime.Now},
               new Event { Name ="My event 1", Location = "Somewhere", StartTime = DateTime.Now }
           };
        }
    }
}