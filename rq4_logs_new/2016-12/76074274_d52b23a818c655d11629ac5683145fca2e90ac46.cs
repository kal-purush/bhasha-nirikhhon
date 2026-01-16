using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using PrototypeAdal5.Data;
using PrototypeAdal5.Models;

namespace PrototypeAdal5.Controllers
{
    public class ReleasesController : Controller
    {
        private readonly ReleaseContext _context;

        public ReleasesController(ReleaseContext context)
        {
            _context = context;    
        }

        // GET: Releases
        public async Task<IActionResult> Index()
        {
            return View(await _context.Releases.ToListAsync());
        }

        // GET: Releases/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var release = await _context.Releases.SingleOrDefaultAsync(m => m.ID == id);
            if (release == null)
            {
                return NotFound();
            }

            return View(release);
        }

        // GET: Releases/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: Releases/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ID,ApprovalStatus,ApprovedBy,ApprovedDate,ProductName,ReleaseNotes,SubmissionDate,VersionNumber")] Release release)
        {
            if (ModelState.IsValid)
            {
                _context.Add(release);
                await _context.SaveChangesAsync();
                return RedirectToAction("Index");
            }
            return View(release);
        }

        // GET: Releases/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var release = await _context.Releases.SingleOrDefaultAsync(m => m.ID == id);
            if (release == null)
            {
                return NotFound();
            }
            return View(release);
        }

        // POST: Releases/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("ID,ApprovalStatus,ApprovedBy,ApprovedDate,ProductName,ReleaseNotes,SubmissionDate,VersionNumber")] Release release)
        {
            if (id != release.ID)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(release);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!ReleaseExists(release.ID))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
                return RedirectToAction("Index");
            }
            return View(release);
        }

        // GET: Releases/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var release = await _context.Releases.SingleOrDefaultAsync(m => m.ID == id);
            if (release == null)
            {
                return NotFound();
            }

            return View(release);
        }

        // POST: Releases/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var release = await _context.Releases.SingleOrDefaultAsync(m => m.ID == id);
            _context.Releases.Remove(release);
            await _context.SaveChangesAsync();
            return RedirectToAction("Index");
        }

        private bool ReleaseExists(int id)
        {
            return _context.Releases.Any(e => e.ID == id);
        }
    }
}