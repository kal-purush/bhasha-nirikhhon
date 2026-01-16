using Resturent.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Resturent.Controllers
{
    public class HomeController : Controller
    {
        // GET: Home
        public ActionResult Index()
        {
            //return RedirectToAction("Login", "User", new { id = 0 });
            var home = new HomeMV();
            return View(home);
        }
    }
}