using FluentValidation;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using dotnet9_ketnoigiaothuong.Domain.Contracts;
using static dotnet9_ketnoigiaothuong.Domain.Contracts.CategoryContract;

namespace dotnet9_ketnoigiaothuong.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CategoryController : BaseController
    {
        private readonly IValidator<CreateCategoryModel> _createCategoryValidator;
        private readonly IValidator<UpdateCategoryModel> _updateCategoryValidator;

        public CategoryController(
            IValidator<CreateCategoryModel> createCategoryValidator,
            IValidator<UpdateCategoryModel> updateCategoryValidator)
        {
            _createCategoryValidator = createCategoryValidator;
            _updateCategoryValidator = updateCategoryValidator;
        }

        [HttpGet]
        public async Task<ActionResult<ApiResponse<List<CategoryListItem>>>> GetAllCategories()
        {
            var result = await Provider.CategoryService.GetAllCategoriesAsync();
            if (!result.IsSuccess)
                return BadRequest(result);
                
            return Ok(result);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ApiResponse<CategoryDetailModel>>> GetCategoryById(int id)
        {
            var result = await Provider.CategoryService.GetCategoryByIdAsync(id);
            if (!result.IsSuccess)
                return NotFound(result);
                
            return Ok(result);
        }

        [HttpPost]
        [Authorize(Roles = "Admin")]
        public async Task<ActionResult<ApiResponse<CategoryDetailModel>>> CreateCategory([FromBody] CreateCategoryModel model)
        {
            var validationResult = await _createCategoryValidator.ValidateAsync(model);
            if (!validationResult.IsValid)
            {
                return BadRequest(validationResult.Errors);
            }

            var result = await Provider.CategoryService.CreateCategoryAsync(model);
            if (!result.IsSuccess)
                return BadRequest(result);
                
            return Ok(result);
        }

        [HttpPut("{id}")]
        [Authorize(Roles = "Admin,Company")]
        public async Task<ActionResult<ApiResponse<CategoryDetailModel>>> UpdateCategory(int id, [FromBody] UpdateCategoryModel model)
        {
            var validationResult = await _updateCategoryValidator.ValidateAsync(model);
            if (!validationResult.IsValid)
            {
                return BadRequest(validationResult.Errors);
            }

            var result = await Provider.CategoryService.UpdateCategoryAsync(id, model);
            if (!result.IsSuccess)
                return BadRequest(result);
                
            return Ok(result);
        }

        [HttpDelete("{id}")]
        [Authorize(Roles = "Admin")]
        public async Task<ActionResult<ApiResponse<bool>>> DeleteCategory(int id)
        {
            var result = await Provider.CategoryService.DeleteCategoryAsync(id);
            if (!result.IsSuccess)
                return BadRequest(result);
                
            return Ok(result);
        }
    }
} 