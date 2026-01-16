using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Recipes.Controllers
{
    [Route("api2/categories")]
    public class CategoriesOfRecipesController : Microsoft.AspNetCore.Mvc.Controller
    {
        [HttpGet]
        public IActionResult GetCategories()
        {
            return Ok(RecipesDataStore.Current.Categories);
        }

        [HttpGet("{id}")]
        public IActionResult GetCategory(int id)
        {
            var categorieToReturn = RecipesDataStore.Current.Categories.FirstOrDefault(c => c.Id == id);
            if (categorieToReturn == null)
            {
                return NotFound();
            }
            return Ok(categorieToReturn);
        }
    }
}