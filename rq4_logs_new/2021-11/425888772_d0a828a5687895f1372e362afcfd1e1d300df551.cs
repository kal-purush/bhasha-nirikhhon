using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace ResturantRater.Controllers
{
    public class ResturantController : Controller
    {
        // GET: Resturant
        public ActionResult Index()
        {
            return View();
        }
    }
}