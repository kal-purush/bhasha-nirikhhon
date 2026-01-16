using EtudeManyToMany.API.Repository;
using EtudeManyToMany.Core.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EtudeManyToMany.API.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class TrajetController : ControllerBase
    {
        private readonly IRepository<Trajet> _trajetRepository;

        public TrajetController(IRepository<Trajet> trajetRepository)
        {
            _trajetRepository = trajetRepository;
        }



        [HttpGet]
        public async Task<IActionResult> ToutLesTrajets()
        {
            return Ok(await _trajetRepository.GetAll());
        }

        [HttpGet("{trajetId}")]
        public async Task<IActionResult> Get(int trajetId)
        {
            return Ok(await _trajetRepository.GetById(trajetId));
        }



    }
}