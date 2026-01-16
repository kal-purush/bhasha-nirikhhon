using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Lab5_Gadgets.Models;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Lab5_Gadgets.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        //view index page
        public IActionResult Index()
        {
            return View();
        }

        //view privacy policy page
        public IActionResult Privacy()
        {
            return View();
        }

        //view contact us page
        public IActionResult contactus()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}