using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using ToyWebsite.DAL;
using ToyWebsite.Models;

namespace ToyWebsite.Controllers
{
    public class SearchController : Controller
    {
        // GET: Search
        [LoggingFilter]
        public ActionResult Index(string searchString)
        {
            StoreContext context = new StoreContext();


            List<Item> searchItems = (from i in context.Items
                                      where i.itemName.Contains(searchString) || i.itemDescription.Contains(searchString)
                                      select i).ToList();

            return View(searchItems);
        }
    }
}