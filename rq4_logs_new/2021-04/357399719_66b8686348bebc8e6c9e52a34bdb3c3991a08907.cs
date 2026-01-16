using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using WebApplicationTeam1.Models;
using WebApplicationTeam1.DataAccess;

namespace WebApplicationTeam1.Controllers
{
    public class HomeController : Controller
    {
        public AppDbContext dbContext; // linking db context to our controller

        public HomeController(AppDbContext context) //linking our controller to the dbcontext for link commands
        {
            dbContext = context;
            //context.Database.EnsureCreated();
        }


        public IActionResult Index() // here's where we create the linq queries for the data ???
        {

            return View();
        }

        //private readonly ILogger<HomeController> _logger;

        //public HomeController(ILogger<HomeController> logger)
        //{
        //    _logger = logger;
        //} was put here bvy default

        //public IActionResult Privacy()
        //{
        //    return View();
        //}was put here by default

        //[ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        //public IActionResult Error()
        //{
        //    return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        //} was put here by default
    }
}