using System.Collections.Generic;
using Microsoft.AspNet.Mvc;
using SpaceshipTracker.Web.Models;
using SpaceshipTracker.Web.Repositories;


// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace SpaceshipTracker.Web.Controllers
{
    [Route("api/[controller]")]
    public class ShipController : Controller
    {        
        private readonly IPositionRepository _positionRepository;

        public ShipController(IPositionRepository postionRepository)
        {
            _positionRepository = postionRepository;
        }

        // GET: api/values
        [HttpGet]
        public IEnumerable<string> Get()
        {            
            return _positionRepository.GetAllShipNames();
            
        }
        
    }
}