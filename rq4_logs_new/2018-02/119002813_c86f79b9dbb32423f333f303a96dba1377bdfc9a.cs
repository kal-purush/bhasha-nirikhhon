using Brewery_Tracker.Models;
using Brewery_Tracker.ViewModels.Home;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Brewery_Tracker.Controllers
{
    public class BeersController : Controller
    {
        // GET: Beers
        public ActionResult BeerList()
        {
            var factory = new BeerFactory();

            var viewModel = new BeerListViewModel(factory.Beers);

            return View(viewModel);
        }
    }
}