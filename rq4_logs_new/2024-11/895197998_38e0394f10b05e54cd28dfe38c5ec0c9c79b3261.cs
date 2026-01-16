using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;

namespace EffiSense.Controllers
{
    [Authorize]
    public class AppliancesController : Controller
    {
        private readonly ApplicationDbContext _context;

        public AppliancesController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: Appliances
        public async Task<IActionResult> Index()
        {
            var applicationDbContext = _context.Appliances.Include(a => a.Home);
            return View(await applicationDbContext.ToListAsync());
        }

        // GET: Appliances/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var appliance = await _context.Appliances
                .Include(a => a.Home)
                .FirstOrDefaultAsync(m => m.ApplianceId == id);
            if (appliance == null)
            {
                return NotFound();
            }

            return View(appliance);
        }

        // GET: Appliances/Create
        public IActionResult Create()
        {
            ViewData["HomeId"] = new SelectList(_context.Homes, "HomeId", "HomeId");
            return View();
        }

        // POST: Appliances/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ApplianceId,HomeId,Name,EnergyConsumption,IsActive")] Appliance appliance)
        {
            ModelState.Remove("Home");
            if (ModelState.IsValid)
            {
                _context.Add(appliance);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["HomeId"] = new SelectList(_context.Homes, "HomeId", "HomeId", appliance.HomeId);
            return View(appliance);
        }

        // GET: Appliances/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var appliance = await _context.Appliances.FindAsync(id);
            if (appliance == null)
            {
                return NotFound();
            }
            ViewData["HomeId"] = new SelectList(_context.Homes, "HomeId", "HomeId", appliance.HomeId);
            return View(appliance);
        }

        // POST: Appliances/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("ApplianceId,HomeId,Name,EnergyConsumption,IsActive")] Appliance appliance)
        {
            ModelState.Remove("Home");
            if (id != appliance.ApplianceId)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(appliance);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!ApplianceExists(appliance.ApplianceId))
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
            ViewData["HomeId"] = new SelectList(_context.Homes, "HomeId", "HomeId", appliance.HomeId);
            return View(appliance);
        }

        // GET: Appliances/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var appliance = await _context.Appliances
                .Include(a => a.Home)
                .FirstOrDefaultAsync(m => m.ApplianceId == id);
            if (appliance == null)
            {
                return NotFound();
            }

            return View(appliance);
        }

        // POST: Appliances/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var appliance = await _context.Appliances.FindAsync(id);
            if (appliance != null)
            {
                _context.Appliances.Remove(appliance);
            }

            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool ApplianceExists(int id)
        {
            return _context.Appliances.Any(e => e.ApplianceId == id);
        }
    }
}