using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Supermarket.API.Domain.Models;
using Supermarket.API.Domain.Services;
using Supermarket.API.Extensions;
using Supermarket.API.Resources;
using Swashbuckle.AspNetCore.Annotations;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Supermarket.API.Controllers
{
    [ApiController]
    [Produces("application/json")]
    [Route("api/[controller]")]
    public class CategoriesController : ControllerBase
    {

        private readonly ICategoryService _categoryService;

        private readonly IMapper _mapper;

        public CategoriesController(ICategoryService categoryService, IMapper mapper)
        {
            _categoryService = categoryService;
            _mapper = mapper;
        }

        [SwaggerOperation(
            Summary = "List all categories",
            Description = "List of categories",
            OperationId = "ListAllCategories",
            Tags = new[] {"Categories"}
            )]
        [SwaggerResponse(200, "List of Categories", typeof(IEnumerable<CategoryResource>))]
        [ProducesResponseType(typeof(IEnumerable<CategoryResource>), 200)]
        [HttpGet]
        public async Task<IEnumerable<CategoryResource>> GetAllAsync()
        {
            var categories = await _categoryService.ListAsync();
            var resources = _mapper.Map<IEnumerable<Category>,
                IEnumerable<CategoryResource>>(categories);
            return resources;
        }

        [SwaggerOperation(
            Summary = "Create a Category",
            Description = "Create a Category",
            OperationId = "CreateCategory",
            Tags = new[] { "Categories" }
        )]
        [SwaggerResponse(200, "Category was created", typeof(CategoryResource))]
        [HttpPost]
        public async Task<IActionResult> PostAsync([FromBody] SaveCategoryResource resource)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState.GetErrorMessages());

            var category = _mapper.Map<SaveCategoryResource, Category>(resource);

            var result = await _categoryService.SaveAsync(category);

            if (!result.Success)
                return BadRequest(result.Message);

            var categoryResource = _mapper.Map<Category, CategoryResource>(result.Resource);

            return Ok(categoryResource);

        }

        [SwaggerOperation(
            Summary = "Update a Category",
            Description = "Update a Category",
            OperationId = "UpdateCategory",
            Tags = new[] { "Categories" }
        )]
        [SwaggerResponse(200, "Category was updated", typeof(CategoryResource))]
        [HttpPut("id")]
        public async Task<IActionResult> PutAsync(int id, [FromBody] SaveCategoryResource resource)
        {
            var category = _mapper.Map<SaveCategoryResource, Category>(resource);
            var result = await _categoryService.UpdateAsync(id, category);

            if (!result.Success)
                return BadRequest(result.Message);
            var categoryResource = _mapper.Map<Category, CategoryResource>(result.Resource);
            return Ok(categoryResource);
        }

    }
}