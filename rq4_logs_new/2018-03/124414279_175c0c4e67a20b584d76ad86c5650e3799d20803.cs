using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using EpicGameAPI.Data;
using EpicGameAPI.Models;

namespace EpicGameAPI.Controllers
{
    [Produces("application/json")]
    [Route("api/Character")]
    public class CharacterController : Controller
    {
        private ApplicationDbContext _context;

        public CharacterController(ApplicationDbContext ctx)
        {
            _context = ctx;
        }

        //GET api/character
        [HttpGet]
        public IActionResult Get()
        {
            var characters = _context.Character.ToList();

            if(characters == null)
            {
                return NotFound();
            }
            return Ok(characters);
        }

        //GET api/character/{id}
        [HttpGet]
        public IActionResult Get(int id)
        {
            var character = _context.Character.Single(c => c.Id == id);

            if(character == null)
            {
                return NotFound();
            }
            return Ok(character);
        }
    }
}