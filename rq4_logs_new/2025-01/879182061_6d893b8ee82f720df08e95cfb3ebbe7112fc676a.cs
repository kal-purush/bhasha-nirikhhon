using Microsoft.AspNetCore.Mvc;
using OnlineLearningWebAPI.DTOs;
using OnlineLearningWebAPI.Service.IService;

namespace OnlineLearningWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CourseCategoryController : ControllerBase
    {
        private readonly ICourseCategoryService _courseCategoryService;

        public CourseCategoryController(ICourseCategoryService courseCategoryService)
        {
            _courseCategoryService = courseCategoryService;
        }

        // GET: api/coursecategories/{id}
        [HttpGet("{id}")]
        public async Task<IActionResult> GetCategory(int id)
        {
            var category = await _courseCategoryService.GetCategoryByIdAsync(id);
            if (category == null)
                return NotFound(new { message = "[CourseCategoryController] | GetCategory | Category not found" });

            return Ok(category);
        }

        // PUT: api/coursecategories/{id}
        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateCategory(int id, [FromBody] CourseCategoryDTO courseCategoryDTO)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var isUpdated = await _courseCategoryService.UpdateCategoryAsync(id, courseCategoryDTO);
            if (!isUpdated)
                return NotFound(new { message = "[CourseCategoryController] | UpdateCategory | Category not found" });

            return NoContent();
        }

        // GET: api/coursecategories
        [HttpGet]
        public async Task<IActionResult> GetAllCategories()
        {
            var categories = await _courseCategoryService.GetAllCategoriesAsync();
            return Ok(categories);
        }
    }
}