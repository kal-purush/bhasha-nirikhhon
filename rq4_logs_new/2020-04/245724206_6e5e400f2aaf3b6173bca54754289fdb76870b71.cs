using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CompetitionApp.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CompetitionApp.Controllers
{
    public class UsersController : Controller
    {
        private readonly ApplicationContext _context;
        private readonly UserManager<User> _userManager;

        public UsersController(UserManager<User> userManager, ApplicationContext context)
        {
            _context = context;
            _userManager = userManager;
        }

        public async Task<IActionResult> Index()
        {
            return View(await _context.Users.ToListAsync());
        }

        public async Task<IActionResult> Edit(string? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var @user = await _context.Users.FindAsync(id);
            if (@user == null)
            {
                return NotFound();
            }
            return View(@user);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(string id, bool isAdmin)
        {
            User user = await _userManager.FindByIdAsync(id);

            if (user != null)
            {
                try
                {
                    user.IsAdmin = isAdmin;

                    IdentityResult result = await _userManager.UpdateAsync(user);
                    if (result.Succeeded)
                        return RedirectToAction("Index");
                }
                catch (Exception)
                {
                    throw;
                }
                return RedirectToAction(nameof(Index));
            }
            else
                ModelState.AddModelError("", "User Not Found");
            return View(user);
        }
    }
}