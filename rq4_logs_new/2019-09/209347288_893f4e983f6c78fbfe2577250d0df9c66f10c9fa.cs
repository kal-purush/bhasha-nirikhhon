using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using prueba20;

namespace AppIntento22.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AnimalsController : ControllerBase
    {
        private readonly bdanimalesContext _context;

        public AnimalsController (bdanimalesContext context)
        {
            _context = context;
        }

        // GET: api/Animals
        [HttpGet]
        public async Task <ActionResult <IEnumerable<Animal>>> GetAnimals()
        {
            var ls = _context.Animal.ToList();
            return ls;
        }


    }
}