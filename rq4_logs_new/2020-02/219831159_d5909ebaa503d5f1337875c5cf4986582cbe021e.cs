using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Training1.Models;
using Training1.Repositories;

namespace Training1.Controllers
{
    public class FoodsController : Controller
    {
        private readonly IFoodRepository _foodRepository;
        public FoodsController(IFoodRepository foodRepository)
        {
            _foodRepository = foodRepository;
        }

        [Produces("application/json")]
        [HttpGet]
        public async Task<IActionResult> Search(string searchText)
        {
            try
            {
                List<Food> foods = await _foodRepository.SearchByNameAsync(searchText);
                return Ok(foods);
            }
            catch
            {
                return BadRequest();
            }
        }

    }
}