using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using ProductsAPI.Models;
using ProductsAPI.Repository;

namespace ProductsAPI.Controllers
{
    [ApiController]
    public class ProductController : ControllerBase
    {
        public ProductController()
        {
        }

        [HttpGet]
        [Route("[controller]/GetAll")]
        public IEnumerable<Product> GetAll()
        {
            return new ProductsRepository().GetAll();
        }

        [HttpGet]
        [Route("[controller]/GetById/{id}")]
        public Product GetById(Guid id)
        {
            return new ProductsRepository().GetById(id);
        }

        [HttpPost]
        [Route("[controller]/ChangeStock/{id}/{newStock}")]
        public void ChangeStock(Guid id, int newStock)
        {
            new ProductsRepository().ChangeStock(id, newStock);
        }

        [HttpPost]
        [Route("[controller]/Add")]
        public Product Add(Product product)
        {
            return new ProductsRepository().Add(product);
        }

        [HttpPost]
        [Route("[controller]/RemoveById/{id}")]
        public Product RemoveById(Guid id)
        {
            return new ProductsRepository().RemoveById(id);
        }
    }
}