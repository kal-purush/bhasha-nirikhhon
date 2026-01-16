using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;

namespace EffiSense.Controllers
{
    [Authorize]
    public class UsagesController : Controller
    {
        private readonly ApplicationDbContext _context;
        private readonly UserManager<ApplicationUser> _userManager;

        public UsagesController(ApplicationDbContext context, UserManager<ApplicationUser> userManager)
        {
            _context = context;
            _userManager = userManager;
        }

        public async Task<IActionResult> Index()
        {
            var user = await _userManager.GetUserAsync(User);

            var applicationDbContext = _context.Usages
                .Where(u => u.UserId == user.Id) 
                .Include(u => u.Appliance)
                .Include(u => u.User);

            return View(await applicationDbContext.ToListAsync());
        }


        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var usage = await _context.Usages
                .Include(u => u.Appliance)
                .Include(u => u.User)
                .FirstOrDefaultAsync(m => m.UsageId == id);

            var user = await _userManager.GetUserAsync(User);
            if (usage == null || usage.UserId != user.Id)
            {
                return Forbid(); 
            }

            return View(usage);
        }

        public IActionResult Create()
        {
            var user = _userManager.GetUserAsync(User).Result;

            ViewData["UserId"] = user.Id; 
            ViewData["ApplianceId"] = new SelectList(_context.Appliances.Where(a => a.Home.UserId == user.Id), "ApplianceId", "Name");
            return View();
        }

        // POST: Usages/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("UsageId,UserId,ApplianceId,Date,EnergyUsed,UsageFrequency,Duration")] Usage usage)
        {
            ModelState.Remove("User");
            ModelState.Remove("Appliance");
            ModelState.Remove("UserId");

            var user = await _userManager.GetUserAsync(User);

            var appliance = await _context.Appliances
                .Include(a => a.Home)
                .FirstOrDefaultAsync(a => a.ApplianceId == usage.ApplianceId);

            if (appliance == null || appliance.Home.UserId != user.Id)
            {
                return Forbid();
            }

            usage.UserId = user.Id;

            if (ModelState.IsValid)
            {
                _context.Add(usage);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }

            ViewData["UserId"] = usage.UserId;
            ViewData["ApplianceId"] = new SelectList(_context.Appliances.Where(a => a.Home.UserId == user.Id), "ApplianceId", "Name", usage.ApplianceId);
            return View(usage);
        }



        // GET: Usages/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var usage = await _context.Usages.FindAsync(id);
            if (usage == null)
            {
                return NotFound();
            }

            var user = await _userManager.GetUserAsync(User);

            if (usage.UserId != user.Id)
            {
                return Forbid(); 
            }

            ViewData["UserId"] = usage.UserId;
            ViewData["ApplianceId"] = new SelectList(_context.Appliances.Where(a => a.Home.UserId == user.Id), "ApplianceId", "Name", usage.ApplianceId);
            return View(usage);
        }

        // POST: Usages/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("UsageId,UserId,ApplianceId,Date,EnergyUsed,UsageFrequency,Duration")] Usage usage)
        {
            ModelState.Remove("User");
            ModelState.Remove("Appliance");
            ModelState.Remove("UserId");
            if (id != usage.UsageId)
            {
                return NotFound();
            }

            var appliance = await _context.Appliances
              .Include(a => a.Home)
              .FirstOrDefaultAsync(a => a.ApplianceId == usage.ApplianceId);

            var user = await _userManager.GetUserAsync(User);
            usage.UserId = user.Id;
            if (usage.UserId != user.Id || appliance.Home.UserId != user.Id)
            {
                return Forbid(); 
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(usage);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!UsageExists(usage.UsageId))
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

            ViewData["UserId"] = usage.UserId;
            ViewData["ApplianceId"] = new SelectList(_context.Appliances.Where(a => a.Home.UserId == user.Id), "ApplianceId", "Name", usage.ApplianceId);
            return View(usage);
        }


        // GET: Usages/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var usage = await _context.Usages
                .Include(u => u.Appliance)
                .Include(u => u.User)
                .FirstOrDefaultAsync(m => m.UsageId == id);

            var user = await _userManager.GetUserAsync(User);
            if (usage == null || usage.UserId != user.Id)
            {
                return NotFound();
            }

            return View(usage);
        }

        // POST: Usages/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var usage = await _context.Usages.FindAsync(id);
            if (usage != null)
            {
                var user = await _userManager.GetUserAsync(User);
                if (usage.UserId != user.Id)
                {
                    return Forbid(); 
                }

                _context.Usages.Remove(usage);
            }

            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }


        private bool UsageExists(int id)
        {
            return _context.Usages.Any(e => e.UsageId == id);
        }
    }
}