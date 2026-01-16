using Microsoft.AspNetCore.Mvc;
using StoreProjectModels.DatabaseModels;
using StoreServices.Interfaces;

namespace StoreAPI.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class CategoryService : ControllerBase
	{
		private readonly ICategoryService categoryService;
		public CategoryService(ICategoryService _categoryService)
		{
			this.categoryService = _categoryService;
		}

		[HttpGet]
		[Route("GetAllCategories")]
		public IActionResult GetAllCategories()
		{
			return Ok(categoryService.GetAllCategories());
		}

		[HttpPost]
		[Route("AddCategory")]
		public IActionResult AddCategory(Category category)
		{
			return Ok(categoryService.AddCategory(category));
		}
	}
}