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
    [Route("api/pathOption")]
    public class PathOptionController : Controller
    {

        private ApplicationDbContext _context;
        public PathOptionController(ApplicationDbContext ctx)
        {
            _context = ctx;
        }
        //Get api/adventure/{id}
        [HttpGet("{id}", Name = "GetSinglePathOption")]

        public IActionResult Get(int id)
        {
            var pathOption = _context.PathOption.SingleOrDefault(p => p.Id == id);

            if(pathOption == null)
            {
                return NotFound();
            }
            return Ok(pathOption);

        }
    }
}