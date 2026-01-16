using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Taskly.Dashboard.App.Data;
using Taskly.Users.Models;
using System.Security.Cryptography;
using BCrypt.Net;

namespace Home.Controllers
{
    public class HomeController : Controller
    {
        private readonly AppDbContext _context;

        public HomeController(AppDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult Register()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Register(User model)
        {
            if (!ModelState.IsValid)
            {
                return View(model);
            }

            if (_context.Users.Any(u => u.Email == model.Email))
            {
                ModelState.AddModelError("Email", "Email already exists.");
                return View(model);
            }

            // Hashed & salted - BCrypt
            model.Password = BCrypt.Net.BCrypt.HashPassword(model.Password);

            model.CreatedAt = DateTime.UtcNow;
            model.UpdatedAt = DateTime.UtcNow;
            model.IsActive = true;
            model.EmailVerified = false;

            _context.Users.Add(model);
            _context.SaveChanges();

            TempData["SuccessMessage"] = "Successful!";

            return RedirectToAction("");
        }
    }
}