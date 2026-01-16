using MarktGuruTask.Entities;
using MarktGuruTask.Models;
using MarktGuruTask.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MarktGuruTask.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Authorize]
    public class ProductController : ControllerBase
    {
        private readonly ProductService _productService;

        public ProductController(ProductService productService)
        {
            _productService = productService;
        }

        [HttpGet]
        [AllowAnonymous]
        public async Task<IActionResult> GetProducts([FromQuery] PaginationModel model)
        {
            var products = await _productService.GetProducts(model.Offset, model.Count);
            return new JsonResult(products);
        }

        [HttpPost]
        public async Task<IActionResult> AddProduct(ProductModel model)
        {
            if (model == null)
            {
                return BadRequest();
            }

            var entity = new Product
            {
                Available = model.Available,
                Description = model.Description,
                Name = model.Name,
                Price = model.Price
            };

            var result = await _productService.Add(entity);

            return new JsonResult(result);
        }
    }
}