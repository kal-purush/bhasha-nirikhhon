using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ECommerceSite.Data;
using ECommerceSite.Models;
using Microsoft.AspNetCore.Mvc;

namespace ECommerceSite.Controllers
{
    public class UserController : Controller
    {
        private readonly ProductContext _context;

        public UserController(ProductContext context)
        {
            _context = context;
        }

        public IActionResult Register()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Register(RegisterViewModel register)
        {
            if (ModelState.IsValid)
            {
                // Map data to user account instance
                UserAccount account = new UserAccount()
                {
                    DateOfBirth = register.DateOfBirth,
                    Email = register.Email,
                    Password = register.Password,
                    UserName = register.Username
                };

                // Add to database
                _context.UserAccounts.Add(account);
                await _context.SaveChangesAsync();

                // redirect to homepage
                return RedirectToAction("Index", "Home");
            }
            return View(register);
        }
    }
}