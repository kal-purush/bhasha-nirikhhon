using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using EpicGameAPI.Data;
using EpicGameAPI.Models;
using Microsoft.EntityFrameworkCore;
using System.Collections.Specialized;
using Microsoft.AspNetCore.Identity;

namespace EpicGameAPI.Controllers
{
    [Produces("application/json")]
    [Route("api/adventure")]
    public class AdventureController : Controller
    {
        private ApplicationDbContext _context;
        public AdventureController(ApplicationDbContext ctx)
        {
            _context = ctx;
        }

        //GET api/adventure
        [HttpGet]
        public IActionResult GetAction()
        {
            var adventures = _context.Adventure.ToList();

            if(adventures == null)
            {
                return NotFound();
            }

            return Ok(adventures);
        }

        //Get api/adventure/{id}
        [HttpGet("{id}", Name = "GetSingleAdventure")]

        public IActionResult Get(int id)
        {
            var adventure = _context.Adventure.SingleOrDefault(a => a.Id == id);

            if(adventure == null)
            {
                return NotFound();
            }
            return Ok(adventure);

        }
    }
}