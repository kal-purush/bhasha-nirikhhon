using DemoWebAPIApp.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DemoWebAPIApp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : ControllerBase
    {
        public static List<Product> products = new List<Product>();

        [HttpGet]
        public IActionResult GetAll() {
            return Ok(products);
        }

        [HttpGet("{id}")]
        public IActionResult GetById(string id)
        {
            try
            {
                Product product = products.SingleOrDefault(pr => pr.ID == Guid.Parse(id));
                if (product == null) {
                    return NotFound();
                }
                return Ok(product);
            }
            catch {
                return BadRequest();
            }
        }

        [HttpPost]
        public IActionResult Create(ProductVM productVM) { 
            var product = new Product{ 
                ID = Guid.NewGuid(),
                Name = productVM.Name,
                Price = productVM.Price
            };
            products.Add(product);
            return Ok(new { 
                Success = true, Data = products
            });
        }
    }
}