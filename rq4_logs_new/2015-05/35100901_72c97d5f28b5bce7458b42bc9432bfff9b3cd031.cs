using Sozialheap.Models;
using Sozialheap.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Sozialheap.Controllers
{
    public class GroupController : Controller
    {
        SozialService service = new SozialService();
        public ActionResult CreateGroup(Group g)
        {
            // TODO: create view!!

            return View();
        }
    }
}