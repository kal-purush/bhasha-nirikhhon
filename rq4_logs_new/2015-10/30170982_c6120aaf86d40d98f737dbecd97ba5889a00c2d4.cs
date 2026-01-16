using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace sonrise.Controllers
{
    public class ErrorController : Controller
    {
        public ActionResult Index()
        {
            ViewBag.Title = "Error Page";
            return View("Error");
        }

        public ActionResult NotFound()
        {
            ViewBag.Title = "Not Found Page";
            return View();
        }
    }
}