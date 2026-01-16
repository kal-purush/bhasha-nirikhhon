using DemoWebAPIApp.Data;
using DemoWebAPIApp.Models;
using DemoWebAPIApp.Services;
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
    public class ProductsController : ControllerBase
    {
        private readonly IProductRepository _productRepository;

        public ProductsController(IProductRepository productRepository)
        {
            _productRepository = productRepository;
        }

     

        [HttpGet]
        public IActionResult GetAll(string search, double? from, double? to, string sortBy, int page = 1)
        {
            try
            {
                var result = _productRepository.GetAll(search, from, to, sortBy, page);
                return Ok(result);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.ToString());
            }
        }

        [HttpPost]
        public IActionResult AddProduct(ProductVM productVM)
        {
            try
            {
                return Ok(_productRepository.AddProduct(productVM));
            }
            catch
            {
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }
    }
}