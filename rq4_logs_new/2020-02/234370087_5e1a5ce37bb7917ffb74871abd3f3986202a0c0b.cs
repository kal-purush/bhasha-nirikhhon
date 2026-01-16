using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using TinyClothes.Data;
using TinyClothes.Models;

namespace TinyClothes.Controllers
{
    public class CartController : Controller
    {
        private readonly StoreContext _context;
        private readonly IHttpContextAccessor _http;

        public CartController(StoreContext context, IHttpContextAccessor http)
        {
            _context = context;
            _http = http;
        }

        // Display all products in cart
        public IActionResult Index()
        {
            return View();
        }

        // Add a single product to the shopping cart
        public async Task<IActionResult> Add(int ID)
        {
            Clothing c = await ClothingDB.GetClothingByID(ID, _context);

            if (c != null)
            {
                CartHelper.Add(c, _http);
            }

            return RedirectToAction("Index", "Home");
        }

        // Summary / checkout page
        public IActionResult Checkout()
        {
            return View();
        }
    }
}