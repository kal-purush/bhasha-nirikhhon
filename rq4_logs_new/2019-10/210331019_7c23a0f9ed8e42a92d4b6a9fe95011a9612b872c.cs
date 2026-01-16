using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Gride.Data;
using Gride.Models;
using Microsoft.AspNetCore.Mvc;

namespace Gride.Controllers
{
    public class RosterController : Controller
    {
        private readonly ApplicationDbContext _context;
        public Schedule schedule = new Schedule();

        public RosterController(ApplicationDbContext context)
        {
            _context = context;
        }
        public IActionResult Index(int? id)
        {
            List<Shift> allShifts = _context.Shift.ToList();

            if (id == null)
            {
                id = schedule._weekNumber;
            }

            schedule.currentWeek = (int)id;
            schedule.setWeek((int)id);
            schedule.makeSchedule();
            schedule.setShifts(allShifts);

            return View(schedule);
        }
    }
}