using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using EveryBook.Data;
using EveryBook.Models;

namespace EveryBook.Controllers
{
    public class DistributionUnitsController : Controller
    {
        private readonly EveryBookContext _context;

        public DistributionUnitsController(EveryBookContext context)
        {
            _context = context;
        }

        // GET: DistributionUnits
        public async Task<IActionResult> Index()
        {
            var everyBookContext = _context.DistributionUnit.Include(d => d.Location);
            return View(await everyBookContext.ToListAsync());
        }

        // GET: DistributionUnits/Details/5
        public async Task<IActionResult> Details(long? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var distributionUnit = await _context.DistributionUnit
                .Include(d => d.Location)
                .FirstOrDefaultAsync(m => m.Id == id);
            if (distributionUnit == null)
            {
                return NotFound();
            }

            return View(distributionUnit);
        }

        // GET: DistributionUnits/Create
        public IActionResult Create()
        {
            ViewData["LocationId"] = new SelectList(_context.Location, "Id", "Address");
            return View();
        }

        // POST: DistributionUnits/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,Name,LocationId")] DistributionUnit distributionUnit)
        {
            if (ModelState.IsValid)
            {
                _context.Add(distributionUnit);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["LocationId"] = new SelectList(_context.Location, "Id", "Address", distributionUnit.LocationId);
            return View(distributionUnit);
        }

        // GET: DistributionUnits/Edit/5
        public async Task<IActionResult> Edit(long? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var distributionUnit = await _context.DistributionUnit.FindAsync(id);
            if (distributionUnit == null)
            {
                return NotFound();
            }
            ViewData["LocationId"] = new SelectList(_context.Location, "Id", "Address", distributionUnit.LocationId);
            return View(distributionUnit);
        }

        // POST: DistributionUnits/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(long id, [Bind("Id,Name,LocationId")] DistributionUnit distributionUnit)
        {
            if (id != distributionUnit.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(distributionUnit);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!DistributionUnitExists(distributionUnit.Id))
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
            ViewData["LocationId"] = new SelectList(_context.Location, "Id", "Address", distributionUnit.LocationId);
            return View(distributionUnit);
        }

        // GET: DistributionUnits/Delete/5
        public async Task<IActionResult> Delete(long? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var distributionUnit = await _context.DistributionUnit
                .Include(d => d.Location)
                .FirstOrDefaultAsync(m => m.Id == id);
            if (distributionUnit == null)
            {
                return NotFound();
            }

            return View(distributionUnit);
        }

        // POST: DistributionUnits/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(long id)
        {
            var distributionUnit = await _context.DistributionUnit.FindAsync(id);
            _context.DistributionUnit.Remove(distributionUnit);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool DistributionUnitExists(long id)
        {
            return _context.DistributionUnit.Any(e => e.Id == id);
        }
    }
}