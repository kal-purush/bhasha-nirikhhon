using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using InterdimensionalThings.Models;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling MVC for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace InterdimensionalThings.Controllers
{
    public class CartController : Controller
    {
        // GET: /<controller>/
        public IActionResult Index()
        {
            ThingCart model = new ThingCart
            {
                ID = 1,
                Things = new Thing[]
                {
                    new Thing{
                        Id = 1, Name = "Eyeholes",
                        Description = "Get up on out of here with my Eyeholes!",
                        Price = 25.99m,
                        ImagePath = "./images/Eyeholes.png",
                        category = "itv" },

                    new Thing
                    {
                        Id = 2,
                        Name = "Strawberry Smiggles",
                        Description = "Here they are!",
                        ImagePath = "./images/strawberrysmiggles.jpeg",
                        Price = 18.43m,
                        category = "prototypes"
                    }
                 }
            };
                    return View(model);
        }
        public IActionResult Remove(int id)
        {
            return RedirectToAction("Index");
        }
    }
}
