using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Habits.Models;

namespace Habits.Controllers
{
    public class HomeController : Controller
    {

        public IActionResult Index()
        {
            return View();
        }

        [HttpGet]
        public IActionResult Add()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Add(TaskResponse tr)
        {
            return View();
        }

        [HttpGet]
        public IActionResult Edit()
        {


            return View("Add");

        }

        [HttpPost]
        public IActionResult Edit(TaskResponse tr)
        {
            return RedirectToAction("Index");
        }

        [HttpGet]
        public IActionResult Delete()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Delete(TaskResponse tr)
        {
            return RedirectToAction("Index");
        }
    }
}