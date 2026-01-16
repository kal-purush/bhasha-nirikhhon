using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DisneyCafe.Data;
using DisneyCafe.Models;
using DisneyCafe.Models.Database;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DisneyCafe.Controllers
{
    public class CartController : Controller
    {
        private readonly IHttpContextAccessor _httpAccessor;
        private readonly ApplicationDbContext _context;
        public CartController(ApplicationDbContext context, IHttpContextAccessor accessor)
        {
            _httpAccessor = accessor;
            _context = context;
        }
        public async Task<IActionResult> Add(int id)
        {
            Desserts dessert = await DessertsDb.GetDessertsById(_context, id);
            CartHelper.Add(_httpAccessor, dessert);
            return RedirectToAction("Catalog", "Order");
        }
        public IActionResult Checkout()
        {
            List<Desserts> desserts = CartHelper.GetDesserts(_httpAccessor);
            return View(desserts);
        }
    }
}