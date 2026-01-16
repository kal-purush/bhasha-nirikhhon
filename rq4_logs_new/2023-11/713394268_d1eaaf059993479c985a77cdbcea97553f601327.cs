using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Garage2._0.Data;
using Garage2._0.Models;

namespace Garage2._0.Controllers
{
    public class ParkeratFordonsController : Controller
    {
        private readonly Garage2_0Context _context;

        public ParkeratFordonsController(Garage2_0Context context)
        {
            _context = context;
        }

        // GET: ParkeratFordons
        public async Task<IActionResult> Index()
        {
              return _context.ParkeratFordon != null ? 
                          View(await _context.ParkeratFordon.ToListAsync()) :
                          Problem("Entity set 'Garage2_0Context.ParkeratFordon'  is null.");
        }

        // GET: ParkeratFordons/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null || _context.ParkeratFordon == null)
            {
                return NotFound();
            }

            var parkeratFordon = await _context.ParkeratFordon
                .FirstOrDefaultAsync(m => m.Id == id);
            if (parkeratFordon == null)
            {
                return NotFound();
            }

            return View(parkeratFordon);
        }

        // GET: ParkeratFordons/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: ParkeratFordons/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,FordonsTyp,RegNr,Farg,Marke,Modell,AntalHjul,AnkomstTid")] ParkeratFordon parkeratFordon)
        {
            if (ModelState.IsValid)
            {
                _context.Add(parkeratFordon);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(parkeratFordon);
        }

        // GET: ParkeratFordons/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null || _context.ParkeratFordon == null)
            {
                return NotFound();
            }

            var parkeratFordon = await _context.ParkeratFordon.FindAsync(id);
            if (parkeratFordon == null)
            {
                return NotFound();
            }
            return View(parkeratFordon);
        }

        // POST: ParkeratFordons/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,FordonsTyp,RegNr,Farg,Marke,Modell,AntalHjul,AnkomstTid")] ParkeratFordon parkeratFordon)
        {
            if (id != parkeratFordon.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(parkeratFordon);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!ParkeratFordonExists(parkeratFordon.Id))
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
            return View(parkeratFordon);
        }

        // GET: ParkeratFordons/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null || _context.ParkeratFordon == null)
            {
                return NotFound();
            }

            var parkeratFordon = await _context.ParkeratFordon
                .FirstOrDefaultAsync(m => m.Id == id);
            if (parkeratFordon == null)
            {
                return NotFound();
            }

            return View(parkeratFordon);
        }

        // POST: ParkeratFordons/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            if (_context.ParkeratFordon == null)
            {
                return Problem("Entity set 'Garage2_0Context.ParkeratFordon'  is null.");
            }
            var parkeratFordon = await _context.ParkeratFordon.FindAsync(id);
            if (parkeratFordon != null)
            {
                _context.ParkeratFordon.Remove(parkeratFordon);
            }
            
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool ParkeratFordonExists(int id)
        {
          return (_context.ParkeratFordon?.Any(e => e.Id == id)).GetValueOrDefault();
        }
    }
}