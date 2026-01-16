namespace BoardGames.Areas.Private.Controllers
{
    using Services.Data.Contracts;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Web;
    using System.Web.Mvc;

    public class TutorialsController : Controller
    {
        private readonly ITutorialsService tutorials;

        public TutorialsController(ITutorialsService tutorials)
        {
            this.tutorials = tutorials;
        }

        // GET: Private/Tutorials
        public ActionResult Index()
        {
            return View();
        }
    }
}