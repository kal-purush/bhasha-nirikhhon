using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using SoftwareStudioMVC.Models;

namespace SoftwareStudioMVC.Controllers
{
    public class BookingController : Controller
    {
        private readonly ILogger<BookingController> _logger;

        public BookingController(ILogger<BookingController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            ViewData["Page1"] = "select";
            ViewData["Page2"] = "unselect";
            ViewData["Page3"] = "unselect";
            return View();
        }

        public IActionResult History()
        {
            ViewData["Page1"] = "unselect";
            ViewData["Page2"] = "unselect";
            ViewData["Page3"] = "select";
            return View();
        }

        public IActionResult Overdue()
        {
            ViewData["Page1"] = "unselect";
            ViewData["Page2"] = "select";
            ViewData["Page3"] = "unselect";
            return View();
        }

    }
}