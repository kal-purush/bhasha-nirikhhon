using AgeApp.Model;
using AgeApp.Service;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace AgeApp.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DetermineController : ControllerBase
    {
        private readonly IDetermineService _determineService;

        public DetermineController(IDetermineService determineService)
        {
            _determineService = determineService;
        }
        [HttpPost]
        public IActionResult ShowAge(DateCollectorModel model)
        { 
         int result = _determineService.ShowAge(model);
         return Ok(result);
        }


    }
}