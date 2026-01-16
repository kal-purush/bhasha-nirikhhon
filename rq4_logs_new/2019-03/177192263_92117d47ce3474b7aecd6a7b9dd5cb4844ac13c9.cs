using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using test;
using test.Data;

namespace test.Controllers.DirectoryLib
{
    public class EducationalInstitutionsController : Controller
    {
        private readonly ApplicationDbContext _context;

        public EducationalInstitutionsController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: EducationalInstitutions
        public async Task<IActionResult> Index()
        {
            var applicationDbContext = _context.EducationalInstitution.Include(e => e.IdTownNavigation);
            return View(await applicationDbContext.ToListAsync());
        }

        // GET: EducationalInstitutions/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var educationalInstitution = await _context.EducationalInstitution
                .Include(e => e.IdTownNavigation)
                .SingleOrDefaultAsync(m => m.IdEducationalInstitution == id);
            if (educationalInstitution == null)
            {
                return NotFound();
            }

            return View(educationalInstitution);
        }

        // GET: EducationalInstitutions/Create
        public IActionResult Create()
        {
            ViewData["IdTown"] = new SelectList(_context.City, "IdTown", "NameCity");
            return View();
        }

        // POST: EducationalInstitutions/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("IdEducationalInstitution,NameEducationalInstitution,IdTown")] EducationalInstitution educationalInstitution)
        {
            if (ModelState.IsValid)
            {
                _context.Add(educationalInstitution);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["IdTown"] = new SelectList(_context.City, "IdTown", "NameCity", educationalInstitution.IdTown);
            return View(educationalInstitution);
        }

        // GET: EducationalInstitutions/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var educationalInstitution = await _context.EducationalInstitution.SingleOrDefaultAsync(m => m.IdEducationalInstitution == id);
            if (educationalInstitution == null)
            {
                return NotFound();
            }
            ViewData["IdTown"] = new SelectList(_context.City, "IdTown", "NameCity", educationalInstitution.IdTown);
            return View(educationalInstitution);
        }

        // POST: EducationalInstitutions/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("IdEducationalInstitution,NameEducationalInstitution,IdTown")] EducationalInstitution educationalInstitution)
        {
            if (id != educationalInstitution.IdEducationalInstitution)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(educationalInstitution);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!EducationalInstitutionExists(educationalInstitution.IdEducationalInstitution))
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
            ViewData["IdTown"] = new SelectList(_context.City, "IdTown", "NameCity", educationalInstitution.IdTown);
            return View(educationalInstitution);
        }

        // GET: EducationalInstitutions/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var educationalInstitution = await _context.EducationalInstitution
                .Include(e => e.IdTownNavigation)
                .SingleOrDefaultAsync(m => m.IdEducationalInstitution == id);
            if (educationalInstitution == null)
            {
                return NotFound();
            }

            return View(educationalInstitution);
        }

        // POST: EducationalInstitutions/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var educationalInstitution = await _context.EducationalInstitution.SingleOrDefaultAsync(m => m.IdEducationalInstitution == id);
            _context.EducationalInstitution.Remove(educationalInstitution);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool EducationalInstitutionExists(int id)
        {
            return _context.EducationalInstitution.Any(e => e.IdEducationalInstitution == id);
        }
    }
}