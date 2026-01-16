using System.Collections.Generic;
using System.Threading.Tasks;
using Green.Models;
using Green.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Green.Controllers
{



    // [Authorize]
    [ApiController]
    [Route("api/[controller]")]
    public class CategoryController : ControllerBase
    {

        private readonly ICategory _category;

        public CategoryController(ICategory category)
        {
            _category = category;

        }



        [AllowAnonymous]
        [HttpGet("all")]
        public async Task<IActionResult> GetAll()
        {
            return Ok(await _category.GetAllCategories());
        }


        [AllowAnonymous]
        [HttpPost]
        public async Task<IActionResult> AddNewProduct(Category newCategory)
        {
            return Ok(await _category.AddNewCategory(newCategory));
        }

        [AllowAnonymous]
        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateCat(Category UpdateCategory, int id)
        {

            SResponse<Category> serviceResponse = await _category.UpdateCategory(UpdateCategory, id);

            if (serviceResponse.Data == null)
            {
                return NotFound(serviceResponse);
            }

            return Ok(serviceResponse);

        }


        [AllowAnonymous]
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            SResponse<List<Category>> serviceResponse = await _category.DeleteCategory(id);

            if (serviceResponse.Data == null)
            {
                return NotFound(serviceResponse);
            }

            return Ok(serviceResponse);
        }

        [AllowAnonymous]
        [HttpGet("{id}")]
        public async Task<IActionResult> GetPCategoryById(int id)
        {
            return Ok(await _category.GetCategoryById(id));
        }
    }
}