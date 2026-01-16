using Api.Common.Routing;
using Api.Controllers.Common;
using Application.Contract.Services;
using Application.DTOs.Product;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    [ApiController]
    public class ProductController : AppControllerBase
    {
        private readonly IProductServices _productServices;
        private readonly ISpecificationServices _specificationServices;

        public ProductController(IProductServices productServices, ISpecificationServices specificationServices)
        {
            _productServices = productServices;
            _specificationServices = specificationServices;
        }

        [HttpGet]
        [Route(ProductRoutes.GetById)]
        public async Task<ActionResult> GetAll([FromRoute] Guid id)
        {
            var response = await _productServices.GetProductByIdAsync(id);

            return ResponseHandler(response);
        }

        [HttpPost]
        [Route(ProductRoutes.Create)]
        public async Task<IActionResult> Create([FromForm] CreateProductDto dto)
        {
            var response = await _productServices.CreateProductAsync(dto);

            return ResponseHandler(response);
        }

        [HttpPut]
        [Route(ProductRoutes.Update)]
        public async Task<IActionResult> Put([FromRoute] Guid id, [FromForm] UpdateProductDto dto)
        {
            var response = await _productServices.UpdateProductAsync(id, dto);

            return ResponseHandler(response);
        }
    }
}