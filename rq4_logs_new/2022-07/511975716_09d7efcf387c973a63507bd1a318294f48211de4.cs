using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using TempPractise.Models;

namespace TempPractise.Controllers
{
    public class HomeController : Controller
    {
       [HttpGet]
        public IActionResult Index()
        {

            return View(new TemperatureModel());
        }
        [HttpPost]
        public IActionResult Temperature(TemperatureModel model)
        {
            return View("temp",model);
        }

        

       
    }
}