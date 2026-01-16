
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using VacationPlanner.ViewModels;
using VacationPlanner.Models;
using VacationPlanner.Data;
using Microsoft.AspNetCore.Http;
using Microsoft.CodeAnalysis;

namespace VacationPlanner.Controllers
{
    public class EventController : Controller
    {
        private readonly ApplicationDbContext _context;
        

        public EventController(ApplicationDbContext dbContext)
        {
            _context = dbContext;
        }

        public IActionResult Event()
        {
            var newEventViewModel = new EventViewModel();
            return View( newEventViewModel );
        }
        

        [HttpPost]
        public IActionResult Event(string[] destination)
        {
            


            int lastCity = _context.City.Max(i => i.ID);
            foreach (var item in destination)
            {

                _context.Events.Add(new Event{Location = item, CityID = lastCity});
                
            }

            _context.SaveChanges();
            return Redirect("/City/ViewCities");
        }

       

        public IActionResult ViewEvents( int id)
        {
            IList<Event> itin = _context.Events.Include(i => i.City).Where(i => i.City.ID == id).ToList();
            
            return View(itin);
        }

        public IActionResult RemoveEvents()
        {
            IList<Event> events = _context.Events.Include( e => e.Location ).ToList();

            return View(events);
        }

        [HttpPost]
        public IActionResult RemoveEvents( int [ ] eventIds )
        {
            foreach ( int eventId in eventIds )
            {
                var theEvent = _context.Events.Single( e => e.ID == eventId );
                _context.Events.Remove( theEvent );
            }

            _context.SaveChanges();

            return Redirect("/City/ViewCities");
        }
        

    }
}


