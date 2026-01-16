using Api.Common.Routing;
using Api.Controllers.Common;
using Application.Contract.Services.CategoryServices;
using Application.DTOs.Category;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    [ApiController]
    public class CategoryController : AppControllerBase
    {
        private readonly ICategoryServices _categoryServices;

        public CategoryController(ICategoryServices categoryServices)
        {
            _categoryServices = categoryServices;
        }

        [HttpGet]
        [Route(CategoryRoutes.GetAll)]
        public async Task<ActionResult> GetAll()
        {
            var response = await _categoryServices.GetAllCategoriesAsync();

            return ResponseHandler(response);
        }

        [HttpGet]
        [Route(CategoryRoutes.GetById)]
        public async Task<ActionResult> GetById([FromRoute] Guid id)
        {
            var response = await _categoryServices.GetCategoryByIdAsync(id);

            return ResponseHandler(response);
        }

        [HttpPost]
        [Route(CategoryRoutes.Create)]
        public async Task<IActionResult> Create([FromForm] CreateCategoryDto dto)
        {
            var response = await _categoryServices.CreateCategoryAsync(dto);

            return ResponseHandler(response);
        }

        [HttpPut]
        [Route(CategoryRoutes.Update)]
        public async Task<IActionResult> Put([FromRoute] Guid id, [FromForm] UpdateCategoryDto dto)
        {
            var response = await _categoryServices.UpdateCategoryAsync(id, dto);

            return ResponseHandler(response);
        }

        [HttpDelete]
        [Route(CategoryRoutes.Delete)]
        public async Task<IActionResult> Delete([FromRoute] Guid id)
        {
            var response = await _categoryServices.DeleteCategoryAsync(id);

            return ResponseHandler(response);
        }
    }
}