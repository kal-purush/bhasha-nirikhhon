using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using CapstoneManager.Repositories;
using System.Collections.Generic;

namespace CapstoneManager.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProgressController : ControllerBase
    {
        private readonly IProgressRepository _progressRepository;
        public ProgressController(IProgressRepository progressRepository)
        {
            _progressRepository = progressRepository;
        }
        [HttpGet]
        public IActionResult GetAll()
        {
            return Ok(_progressRepository.GetProgressTypes());
        }
    }
}