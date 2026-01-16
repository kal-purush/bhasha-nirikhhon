using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DisneyCafe.Data;
using DisneyCafe.Models.Database;
using Microsoft.AspNetCore.Mvc;

namespace DisneyCafe.Controllers
{
    public class OrderController : Controller
    {
        private readonly ApplicationDbContext _context;
        public OrderController(ApplicationDbContext context)
        {
            _context = context;
        }
        public async Task<IActionResult> Catalog()
        {
            List<Desserts> desserts = await DessertsDb.GetDesertsAsync(_context);
            return View(desserts);
        }
    }
}