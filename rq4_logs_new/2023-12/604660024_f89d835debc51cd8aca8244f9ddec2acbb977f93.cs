using Dashboard.Data;
using Dashboard.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;

namespace Dashboard.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        private readonly ApplicationDbContext _dbContext;

        public HomeController(ILogger<HomeController> logger, ApplicationDbContext dbContext)
        {
            _logger = logger;
            _dbContext = dbContext;
        }

        public async Task<IActionResult> Index()
        {
            await _dbContext.SaveChangesAsync();
            ViewBag.List=await _dbContext.Operation.ToListAsync();
            return _dbContext.InventoryItems != null ?
                          View(await _dbContext.InventoryItems.ToListAsync()) :
                          Problem("Entity set 'ApplicationDbContext.InventoryItems'  is null.");
        }

        public IActionResult Privacy()
        {
            return View();
        }
        public async Task<IActionResult> ShowFilteredResults(string SearchName)
        {
            ViewBag.Message = "No such item as '" + SearchName + "' in the Inventory!";
            return _dbContext.InventoryItems != null ?
                View(await _dbContext.InventoryItems.Where(d => d.name.Contains(SearchName)).ToListAsync()) :
                View();

        }
        public IActionResult Reciept()
        {
            return View();
        }


        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}