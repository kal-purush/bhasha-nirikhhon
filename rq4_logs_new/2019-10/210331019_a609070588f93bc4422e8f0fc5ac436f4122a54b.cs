using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Gride.Data;
using Gride.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Gride.Controllers
{
    public class AvailabilityController : Controller { 

        private readonly ApplicationDbContext _context;

        public AvailabilityController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: Availability
        public async Task<IActionResult> Index()
        {
            EmployeeModel employee = _context.EmployeeModel
                .Single(e => e.EMail == User.Identity.Name);

            ICollection<EmployeeAvailability> employeeAvailabilities = await _context.EmployeeAvailabilities
                .Where(e => e.Employee == employee)
                .Include(e => e.Employee)
                .Include(e => e.Availability)
                .AsNoTracking()
                .ToListAsync();

            List<Availability> allAvailabilities = new List<Availability>();

            foreach (EmployeeAvailability ea in employeeAvailabilities)
            {
                allAvailabilities.Add(ea.Availability);
            }

            int weekNr = DateTime.Now.DayOfYear / 7;
            IEnumerable<Availability> availabilities = allAvailabilities.Where(a => (a.Start.DayOfYear / 7) == weekNr);

            return View(availabilities);
        }

        // GET: Availability/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: Availability/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("AvailabilityID,Start, End")] Availability availability)
        {
            if (ModelState.IsValid)
            {
                EmployeeModel employee = _context.EmployeeModel
                    .Single(e => e.EMail == User.Identity.Name);

                _context.Add(availability);
                _context.SaveChanges();

                EmployeeAvailability employeeAvailability = new EmployeeAvailability
                {
                    AvailabilityID = availability.AvailabilityID,
                    EmployeeID = employee.ID,
                    Availability = availability,
                    Employee = employee
                };
                _context.Add(employeeAvailability);
                await _context.SaveChangesAsync();

                return RedirectToAction(nameof(Index));
            }
            return View(availability);
        }

        // GET: Availability/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var availability = await _context.Availabilities.FindAsync(id);
            if (availability == null)
            {
                return NotFound();
            }
            return View(availability);
        }

        // POST: Availability/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("AvailabilityID,Start,End")] Availability availability)
        {
            if (id != availability.AvailabilityID)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(availability);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!AvailabilityExists(availability.AvailabilityID))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
                return RedirectToAction(nameof(Index));
            }
            return View(availability);
        }

        // GET: Availability/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var availability = await _context.Availabilities
                .FirstOrDefaultAsync(m => m.AvailabilityID == id);
            if (availability == null)
            {
                return NotFound();
            }

            return View(availability);
        }

        // POST: Availability/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var availability = await _context.Availabilities.FindAsync(id);
            _context.Availabilities.Remove(availability);

            var employeeAvailability = _context.EmployeeAvailabilities.Single(e => e.AvailabilityID == id);
            _context.EmployeeAvailabilities.Remove(employeeAvailability);

            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool AvailabilityExists(int id)
        {
            return _context.Availabilities.Any(e => e.AvailabilityID == id);
        }
    }
}