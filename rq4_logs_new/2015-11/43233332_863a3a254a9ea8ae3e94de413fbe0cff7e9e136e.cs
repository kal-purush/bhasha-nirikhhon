using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Craigslist.Controllers
{
    public class DeleteAdController : Controller
    {
        // GET: DeleteAd
        public ActionResult DeleteAd()
        {
            return View();
        }
        public ActionResult AdInactive()
        {
            return View();
        }
    }
}