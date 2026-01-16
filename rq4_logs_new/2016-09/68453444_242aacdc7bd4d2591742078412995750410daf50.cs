using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using TheWorld.Models;
using TheWorld.ViewModels;

namespace TheWorld.Controllers.Api
{
    [Route("api/trips")]
    public class TripsController : Controller
    {
        private readonly IWorldRepository _repo;

        public TripsController(IWorldRepository repo)
        {
            _repo = repo;
        }

        [HttpGet("")]
        public IActionResult Get()
        {
            //if (true) return BadRequest("Bad things happened");

            return Ok(_repo.GetAllTrips());
        }

        [HttpPost("")]
        public IActionResult Post([FromBody]TripViewModel theTrip)
        {
            if (ModelState.IsValid)
            {
                //Save to the DataBase

                return Created($"api/trips/{theTrip.Name}",true);
            }

            return BadRequest("Bad data"); //if private api then return BadRequest(ModelState);

        }
    }
}