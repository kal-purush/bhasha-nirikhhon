using System.Collections.Generic;
using Microsoft.AspNet.Mvc;
using SpaceshipTracker.Web.Models;
using SpaceshipTracker.Web.Repositories;


// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace SpaceshipTracker.Web.Controllers
{
    [Route("api/[controller]")]
    public class PositionController : Controller
    {        
        private readonly IPositionRepository _positionRepository;

        public PositionController(IPositionRepository postionRepository)
        {
            _positionRepository = postionRepository;
        }

        // GET: api/values
        [HttpGet]
        public IEnumerable<Position> Get(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                return _positionRepository.GetAll();
            else
                return _positionRepository.GetAllByIdentifier(identifier);
        }

        [HttpPost]
        public void CreatePosition([FromBody] Position item)
        {
            if (!ModelState.IsValid)
            {                
                Response.StatusCode = 400;
            }
            else
            {

                _positionRepository.Save(item);
                Response.StatusCode = 200;
            }
        }
    }
}