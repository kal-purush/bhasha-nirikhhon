using FormsApp.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace FormsApp.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View(ProductRepository.Products);
        }

        [HttpGet]
        public IActionResult Create()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Create(string Name, string Description, decimal Price, bool isApproved)
        {
            /*Product product*/
            var productAll = ProductRepository.Products.Last();
            int productLast = productAll.Id+1;
            ProductRepository.AddProduct(productLast,Name,Description,Price,isApproved);
            return RedirectToAction("Index");
        }

        [HttpGet]
        public IActionResult Search(string q)
        {
            //gelen q değeri ile arama işlemleri yapılır.
            if (string.IsNullOrWhiteSpace(q))
            {
                return View();
            }
            else
            {
                return View("Index",ProductRepository.Products.Where(i=>i.Name.Contains(q)));
            }
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}