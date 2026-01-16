using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using BigProject.Models;
using BigProject.Services;
using BigProject.Data.Entities;

namespace BigProject.Controllers
{
    public class HomeController : Controller
    {
        private IMailService _mailService = null;
        private BigProjectContext _context = null;

        public HomeController (IMailService mailService, BigProjectContext context)
        {
            _mailService = mailService;
            _context = context;
        }

        public IActionResult Index()
        {
            return View();
        }

         
        public IActionResult About()
        {
            ViewData["Message"] = "Your application description page.";

            return View();
        }

        
        public IActionResult Contact(ContactModel form)
        {
            return View();
        }

        [HttpGet("Contact")]
        public IActionResult Contact()
        {
            ViewData["Message"] = "Your contact page.";

            return View();
        }

        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        public IActionResult Shop()
        {
            var results = from p in _context.Products
                          orderby p.Category
                          select p;
                          
            return View(results.ToList());
        }
    }
}