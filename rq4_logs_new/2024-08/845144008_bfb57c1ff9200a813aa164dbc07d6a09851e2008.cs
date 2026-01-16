using Api.MasterChef.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;

namespace Api.MasterChef.Controllers
{
	[Route("api/v1/[controller]")]
	[ApiController]
	public class CategoriesController : ControllerBase
	{
		private readonly IMemoryCache _cache;

		public CategoriesController(IMemoryCache cache)
		{
			_cache = cache;

				PopularCategorias();
		}
		 private void PopularCategorias()
		{
			var cacheKey = "AllCategories";

			if (!_cache.TryGetValue(cacheKey, out List<CategoryDto> allCategories))
			{
				allCategories= new List<CategoryDto>();
				allCategories.Add(new CategoryDto() { Id = 1, Name = "Buteco" });
				allCategories.Add(new CategoryDto() { Id = 2, Name = "Festa Junina" });

				var cacheEntryOptions = new MemoryCacheEntryOptions
				{
					AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1)
				};
				_cache.Set(cacheKey, allCategories, cacheEntryOptions);
			}

		}

		[HttpPost]
		public async Task<ActionResult<CategoryDto>> CreateCategory(CategoryDto categoryDto)
		{
			var cacheKey = "AllCategories";

			if (!_cache.TryGetValue(cacheKey, out List<CategoryDto> allCategories))
				allCategories = new List<CategoryDto>();

			var maxId = allCategories.DefaultIfEmpty().Max(r => r?.Id ?? 0);
			categoryDto.Id = maxId + 1;

			allCategories.Add(categoryDto);

			var cacheEntryOptions = new MemoryCacheEntryOptions
			{
				AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1)
			};

			_cache.Set(cacheKey, allCategories, cacheEntryOptions);

			return CreatedAtAction(nameof(GetCategory), new { id = categoryDto.Id }, categoryDto);
		}

		[HttpGet("{id}")]
		public async Task<ActionResult<CategoryDto>> GetCategory(int id)
		{
			var cacheKey = "AllCategories";

			if (_cache.TryGetValue(cacheKey, out List<CategoryDto> allCategories))
			{
				var category = allCategories.FirstOrDefault(r => r.Id == id);

				if (category != null)
					return category;
			}

			return NotFound();
		}

		[HttpPut()]
		public async Task<IActionResult> UpdateRecipe(CategoryDto updatedCategory)
		{
			var cacheKey = "AllCategories";

			if (_cache.TryGetValue(cacheKey, out List<CategoryDto> allCategories))
			{
				var existingCategory = allCategories.FirstOrDefault(r => r.Id == updatedCategory.Id);

				if (existingCategory != null)
				{
					existingCategory.Name = updatedCategory.Name;
					existingCategory.Name = updatedCategory.Name;

					_cache.Set(cacheKey, allCategories);
					return Ok(existingCategory);

				}

				return NoContent();
			}

			return NotFound();
		}


		[HttpDelete("{id}")]
		public async Task<IActionResult> DeleteCategory(int id)
		{
			var cacheKey = "AllCategories";

			if (_cache.TryGetValue(cacheKey, out List<CategoryDto> allCategories))
			{
				var categoryToRemove = allCategories.FirstOrDefault(r => r.Id == id);

				if (categoryToRemove != null)
				{
					allCategories.Remove(categoryToRemove);

					_cache.Set(cacheKey, allCategories);
					return Ok(true);
				}

				return NoContent();
			}

			return NotFound();
		}

		[HttpGet]
		public async Task<ActionResult<IEnumerable<CategoryDto>>> GetAllCategories()
		{
			var cacheKey = "AllCategories";

			if (_cache.TryGetValue(cacheKey, out List<CategoryDto> allCategories))
				return allCategories;

			return new List<CategoryDto>();
		}
	}
}