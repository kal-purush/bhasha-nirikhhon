using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using TondForooshApi.Models;
using System.Collections.Generic;
using System.Linq;

namespace TondForooshApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HomeController : ControllerBase
    {
        private readonly TFDbContext _context;

        public HomeController(TFDbContext context)
        {
            _context = context;
        }

        [HttpGet("products")]
        public ActionResult<IEnumerable<Product>> GetProducts()
        {
            var products = _context.Products.ToList();
            return Ok(products);
        }
    }
}