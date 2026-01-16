using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using princeshop.Data;

namespace princeshop.Controllers
{
    public class ProductoController : Controller
    {
        private readonly ILogger<ProductoController> _logger;
        private readonly ApplicationDbContext _context;

        public ProductoController(ILogger<ProductoController> logger,ApplicationDbContext context)
        {
            _logger = logger;
            _context = context;
            
        }

        public async Task<IActionResult> Index()
        {
            var productos= from o in _context.DataProductos select o;
            return View(await productos.ToListAsync());
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View("Error!");
        }
    }
}