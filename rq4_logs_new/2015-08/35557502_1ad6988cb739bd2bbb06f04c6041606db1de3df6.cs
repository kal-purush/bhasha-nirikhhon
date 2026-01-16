using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace LSCS.Web.Controllers
{
    [RoutePrefix("checklists")]
    public class ChecklistController : Controller
    {
        [Route]
        [Route("~/", Name = "default")]
        [HttpGet]
        [AllowAnonymous]
        // GET: Checklist
        public ActionResult Index()
        {
            return View();
        }

        [Route("{id}")]
        [HttpGet]
        [AllowAnonymous]
        public ActionResult Show(Guid id)
        {
            ViewBag.Id = id;
            return View();
        }
    }
}