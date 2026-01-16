using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NWCodeFirstMVC.App.Contracts;
using NWCodeFirstMVCSacffold.Models;

namespace NWCodeFirstMVC.Api.Controllers
{
    [Route("[controller]")]
    [ApiController]

    public class ProductController : ControllerBase
    {
        private readonly IProductService _productService;

        public ProductController(IProductService productService)
        {
            _productService = productService;
        }

        [HttpGet("GetProd")]
        public IActionResult GetAllProduct()
        {
            return Ok(_productService.GetAllProduct());
        }
    }
}