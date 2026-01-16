using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using EdumindAkademia.Models;

namespace EdumindAkademia.Controllers
{
    public class KomentetController : Controller
    {
        private readonly DB _context;

        public KomentetController(DB context)
        {
            _context = context;
        }

        // GET: Komentets
        public async Task<IActionResult> Index()
        {
            var dB = _context.Komentet.Include(k => k.Produktet);
            return View(await dB.ToListAsync());
        }

        // GET: Komentets/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var komentet = await _context.Komentet
                .Include(k => k.Produktet)
                .FirstOrDefaultAsync(m => m.Numri == id);
            if (komentet == null)
            {
                return NotFound();
            }

            return View(komentet);
        }

        // GET: Komentets/Create
        public IActionResult Create()
        {
            ViewData["NumriProduktit"] = new SelectList(_context.Produktet, "Numri", "Numri");
            return View();
        }

        // POST: Komentets/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        //[ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(string txtkomenti, int numriProduktit)
        {
            if (ModelState.IsValid)
            {
                Komentet komentiRi = new Komentet()
                {
                    Komenti = txtkomenti,
                    DataKrijimit = DateTime.Now,
                    Eaprovuar = true,
                    NumriProduktit = numriProduktit
                };
                _context.Add(komentiRi);
                await _context.SaveChangesAsync();
                return Ok(komentiRi);
            }
            //ViewData["NumriProduktit"] = new SelectList(_context.Produktet, "Numri", "Numri", komentet.NumriProduktit);
            return View();
        }

        // GET: Komentets/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var komentet = await _context.Komentet.FindAsync(id);
            if (komentet == null)
            {
                return NotFound();
            }
            ViewData["NumriProduktit"] = new SelectList(_context.Produktet, "Numri", "Numri", komentet.NumriProduktit);
            return View(komentet);
        }

        // POST: Komentets/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Numri,Komenti,Eaprovuar,DataKrijimit,NumriProduktit")] Komentet komentet)
        {
            if (id != komentet.Numri)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(komentet);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!KomentetExists(komentet.Numri))
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
            ViewData["NumriProduktit"] = new SelectList(_context.Produktet, "Numri", "Numri", komentet.NumriProduktit);
            return View(komentet);
        }

        // GET: Komentets/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var komentet = await _context.Komentet
                .Include(k => k.Produktet)
                .FirstOrDefaultAsync(m => m.Numri == id);
            if (komentet == null)
            {
                return NotFound();
            }

            return View(komentet);
        }

        // POST: Komentets/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var komentet = await _context.Komentet.FindAsync(id);
            if (komentet != null)
            {
                _context.Komentet.Remove(komentet);
            }

            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool KomentetExists(int id)
        {
            return _context.Komentet.Any(e => e.Numri == id);
        }
    }
}