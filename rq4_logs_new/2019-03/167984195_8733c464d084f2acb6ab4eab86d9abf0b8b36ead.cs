using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using TRPR.Data;
using TRPR.Models;
using TRPR.Utilities;

namespace TRPR.Controllers
{
    [Authorize]
    public class ResearcherProfileController : Controller
    {
        private readonly TRPRContext _context;

        public ResearcherProfileController(TRPRContext context)
        {
            _context = context;
        }

        // GET: ResearcherProfile
        public IActionResult Index()
        {
            return RedirectToAction(nameof(Details));
        }

        // GET: ResearcherProfile/Details/5
        public async Task<IActionResult> Details()
        {

            var researcher = await _context.Researchers
                .Include(r => r.ResearchInstitutes)
                .ThenInclude(ri => ri.Institute)
                .Where(c => c.ResEmail == User.Identity.Name)
                .FirstOrDefaultAsync();
            if (researcher == null)
            {
                return RedirectToAction(nameof(Create));
            }

            return View(researcher);
        }

        // GET: ResearcherProfile/Create
        public IActionResult Create()
        {

            return View();
        }

        // POST: ResearcherProfile/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ID,ResFirst,ResLast,ResEmail")] Researcher researcher)
        {
            researcher.ResEmail = User.Identity.Name;
            try
            {
                if (ModelState.IsValid)
                {
                    _context.Add(researcher);
                    await _context.SaveChangesAsync();
                    UpdateUserNameCookie(researcher.FullName);
                    return RedirectToAction(nameof(Details));
                }
            }
            catch (DbUpdateException)
            {
                ModelState.AddModelError("", "Unable to save changes. Try again, and if the problem persists see your system administrator.");
            }

            return View(researcher);
        }

        // GET: EmployeeProfile/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var researcher = await _context.Researchers
                .Include(r => r.ResearchInstitutes)
                .ThenInclude(ri => ri.Institute)
                .Where(c => c.ResEmail == User.Identity.Name)
                .FirstOrDefaultAsync();
            if (researcher == null)
            {
                return NotFound();
            }
            return View(researcher);
        }

        // POST: ResearcherProfile/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id)
        {
            var researcherToUpdate = await _context.Researchers
                .FirstOrDefaultAsync(m => m.ID == id);

            if (await TryUpdateModelAsync<Researcher>(researcherToUpdate, "",
                c => c.ResFirst, c => c.ResLast, c => c.ResEmail))
            {
                try
                {
                    _context.Update(researcherToUpdate);
                    await _context.SaveChangesAsync();
                    UpdateUserNameCookie(researcherToUpdate.FullName);
                    return RedirectToAction(nameof(Details));
                }
                catch (DbUpdateException)
                {
                    return NotFound(); 
                }
            }
            return View(researcherToUpdate);
        }

        private void UpdateUserNameCookie(string userName)
        {
            CookieHelper.CookieSet(HttpContext, "userName", userName, 960);
        }

        private bool ResearcherExists(int id)
        {
            return _context.Researchers.Any(e => e.ID == id);
        }
    }
}