using backend.DTOs;
using backend.Services.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace backend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CuisineController : ControllerBase
    {
        private readonly ICuisineService _cuisineService;

        public CuisineController(ICuisineService cuisineService)
        {
            _cuisineService = cuisineService;
        }

        [HttpPost("add-cuisine")]
        public IActionResult AddCuisine(CuisineDto cuisine)
        {
            bool result = _cuisineService.AddCuisine(cuisine);
            if (result)
            {
                return StatusCode(200, new { message = "Cuisine Added Successfuly" });
            }
            return StatusCode(400, new { message = "Cuisine Already Exists" });
        }

        [HttpGet("get-all-cuisines")]
        public IActionResult GetAllCuisines()
        {
            List<CuisineDto> cuisineDtoList = _cuisineService.GetAllCuisines();
            return StatusCode(200, new { cuisines = cuisineDtoList });
        }
    }
}