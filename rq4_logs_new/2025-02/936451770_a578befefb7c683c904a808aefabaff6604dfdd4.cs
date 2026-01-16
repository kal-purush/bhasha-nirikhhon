using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EShop.Presentation.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductsController : ControllerBase
    {
        private Product[] products =
           [
                new Product { Id = 1, Name = "Iphone", Price = 333 },
                new Product { Id = 2, Name = "Iphone1", Price = 444 },
                new Product { Id = 3, Name = "Iphone2", Price = 555 },
                new Product { Id = 4, Name = "Iphone3", Price = 666 },
                new Product { Id = 5, Name = "Iphone4", Price = 777 }
           ];

        [HttpGet]
        public IEnumerable<Product> Get()
        {
            return products;
        }

    }
}