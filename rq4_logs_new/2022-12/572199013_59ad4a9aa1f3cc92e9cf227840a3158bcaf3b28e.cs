using BethanysPieShopMobile.API.Repository;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace PieShopMobile.API.Controllers
{
    [Route("api/[controller]")]
    public class PiesController : Controller
    {
        private readonly PieRepository _pieRepository;

        public PiesController(PieRepository pieRepository)
        {
            _pieRepository = pieRepository;
        }

        [HttpGet]
        public IActionResult GetAllPies()
        {
            return Ok(_pieRepository.GetAllPies());
        }

        [HttpGet("categories")]
        public IActionResult GetCategoriesWithPies()
        {
            return Ok(_pieRepository.GetCategoriesWithPies());
        }

        [HttpGet("{id}")]
        public IActionResult GetPieById(int id)
        {
            return Ok(_pieRepository.GetPieById(id));
        }
    }
}