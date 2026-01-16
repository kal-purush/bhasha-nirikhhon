using System;
using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using FoodInventoryApp.Models;

namespace FoodInventoryApp.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            var model = new InventoryItemsViewModel();
            model.InventoryItems.Add(new InventoryItem()
            {
                Name = "Milk",
                Qty = 1,
                Location = "Fridge",
                UnitsOfMeasurement = "Gallons",
                ExpirationDate = new DateTime(2020, 1, 15)
            });
            model.InventoryItems.Add(new InventoryItem()
            {
                Name = "Eggs",
                Qty = 12,
                Location = "Fridge",
                UnitsOfMeasurement = "Eggs",
                ExpirationDate = new DateTime(2020, 1, 15)
            });
            return View(model);
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}