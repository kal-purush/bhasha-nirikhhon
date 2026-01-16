using Api.Common.Routing;
using Api.Controllers.Common;
using Application.Contract.Services.BrandServices;
using Application.DTOs.Brand;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    [ApiController]
    public class BrandController : AppControllerBase
    {
        private readonly IBrandServices _brandServices;

        public BrandController(IBrandServices brandServices)
        {
            _brandServices = brandServices;
        }

        [HttpGet]
        [Route(BrandRoutes.GetById)]
        public async Task<ActionResult> GetById([FromRoute] Guid id)
        {
            var response = await _brandServices.GetBrandByIdAsync(id);

            return ResponseHandler(response);
        }

        //[HttpGet]
        //[Route(BrandRoutes.GetByName)]
        //public async Task<ActionResult> GetByName([FromQuery] string name)
        //{
        //    var response = await _brandServices.GetBrandByNameAsync(name);

        //    return ResponseHandler(response);
        //}

        [HttpGet]
        [Route(BrandRoutes.GetAll)]
        public async Task<ActionResult> GetAll()
        {
            var response = await _brandServices.GetAllBrandsAsync();

            return ResponseHandler(response);
        }

        [HttpPost]
        [Route(BrandRoutes.Create)]
        public async Task<IActionResult> CreateBrand([FromForm] CreateBrandDto dto)
        {
            var response = await _brandServices.CreateBrandAsync(dto);

            return ResponseHandler(response);
        }

        [HttpDelete]
        [Route(BrandRoutes.Delete)]
        public async Task<IActionResult> DeleteBrand([FromRoute] Guid id)
        {
            var response = await _brandServices.DeleteBrandAsync(id);

            return ResponseHandler(response);
        }

        [HttpPut]
        [Route(BrandRoutes.Update)]
        public async Task<IActionResult> UpdateBrand([FromRoute] Guid id, [FromForm] UpdateBrandDto dto)
        {
            var response = await _brandServices.UpdateBrandAsync(id, dto);

            return ResponseHandler(response);
        }
    }
}