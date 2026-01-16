using Appota.Controllers;
using Appota.Data;
using Appota.Models;
using Microsoft.AspNetCore.Mvc;

namespace Appota.Areas.Admin.Controllers
{
    [Area("Admin")]
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IConfiguration _configuration;
        public ApplicationDbContext db;

        public HomeController(ILogger<HomeController> logger, IConfiguration configuration, ApplicationDbContext db)
        {
            _logger = logger;
            _configuration = configuration;
            this.db = db;
        }

        public IActionResult Index()
        {
            return View();
        }
    }
}