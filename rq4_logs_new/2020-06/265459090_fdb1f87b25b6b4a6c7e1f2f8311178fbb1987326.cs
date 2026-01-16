using System.Linq;
using Microsoft.AspNetCore.Mvc;
using WebServer.Models;

namespace WebServer.Controllers
{
    [Route("api/[controller]")]
    public class ProductsController : Controller
    {
        [HttpGet]
        public ActionResult Get() {
            if (FakeData.Products != null) {
                return Ok(FakeData.Products.Values.ToArray());
            } else {
                return NotFound();
            }
        }

        [HttpGet("{id}")]
        public ActionResult Get(int id) {
            if (FakeData.Products != null && FakeData.Products.ContainsKey(id)) {
                return Ok(FakeData.Products[id]);
            } else {
                return NotFound();
            }
        }

        [HttpGet("price/{low}/{high}")]
        public ActionResult GetAction(int low, int high) {
            var products = FakeData.Products.Values.Where(p => p.Price >= low && p.Price <= high).ToArray();
            if (products.Length > 0) {
                return Ok(products);
            } else {
                return NotFound();
            }
        }

        [HttpDelete("{id}")]
        public ActionResult Delete(int id) {
            if (FakeData.Products != null && FakeData.Products.ContainsKey(id)) {
                FakeData.Products.Remove(id);
                return Ok();
            } else {
                return NotFound();
            }
        }

        [HttpPost]
        public ActionResult Post([FromBody] Product product) {
            product.ID = FakeData.Products.Keys.Max() + 1;
            FakeData.Products.Add(product.ID, product);
            return Created($"api/products/{product.ID}", product);
        }

        [HttpPut("{id}")]
        public ActionResult Put(int id, [FromBody] Product product) {
            if (FakeData.Products.ContainsKey(id)) {
                var prod = FakeData.Products[id];
                prod.ID = product.ID;
                prod.Name = product.Name;
                prod.Price = product.Price;
                return Ok();
            } else {
                return NotFound();
            }
        }

        [HttpPut("raise/{amount}")]
        public ActionResult Put(int amount) {
            foreach(var product in FakeData.Products) {
                product.Value.Price += amount;
            }
            return Ok();
        }
    }
}