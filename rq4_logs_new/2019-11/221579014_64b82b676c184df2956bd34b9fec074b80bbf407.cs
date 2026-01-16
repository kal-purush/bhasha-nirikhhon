using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ChajdPizzaWebApp.Data;
using ChajdPizzaWebApp.Models;

namespace ChajdPizzaWebApp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PizzaTypesController : ControllerBase
    {
        private readonly PizzaTypesRepo _repo;

        public PizzaTypesController(PizzaTypesRepo context)
        {
            _repo = context;
        }

        // GET: api/PizzaTypes
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Size>>> GetSize()
        {
            return await _repo.GetPizzaSizes();
        }

        // GET: api/PizzaTypes/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Size>> GetSize(int id)
        {
            var size = await _repo.Size.FindAsync(id);

            if (size == null)
            {
                return NotFound();
            }

            return size;
        }

        private bool SizeExists(int id)
        {
            return _repo.Size.Any(e => e.Id == id);
        }
    }
}