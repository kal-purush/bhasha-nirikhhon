using BlinkIt.Service.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace BlinkIt.Gateway.Controllers
{
    [ApiController]
    [Route("api/blinkit/[controller]")]
    public class CategoryController : ControllerBase
    {
        private readonly ICategoryService _categoryService;
        public CategoryController(ICategoryService categoryService)
        {
            _categoryService = categoryService;
        }
        
        [HttpGet("category")]
        public IActionResult GetCategories()
        {
            var products = _categoryService.GetCategory();
            if (products == null)
            {
                return NotFound("No products found for the given category.");
            }
            return Ok(products);
        }
    }
    
}