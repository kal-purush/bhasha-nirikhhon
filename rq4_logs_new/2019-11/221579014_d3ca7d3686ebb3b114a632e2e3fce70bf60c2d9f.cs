using ChajdPizzaWebApp.Data;
using ChajdPizzaWebApp.Models;
using ChajdPizzaWebApp.Repositories.Interfaces;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ChajdPizzaWebApp.Controllers
{
    [EnableCors]
    [Route("api/[controller]")]
    [ApiController]
    public class StateApiController : ControllerBase
    {
        private readonly IStateRepo _repo;

        public StateApiController(IStateRepo repo)
        {
            _repo = repo;
        }

        // GET: api/PizzaTypes/sizes
        [HttpGet]
        public async Task<ActionResult<IEnumerable<State>>> GetStates()
        {
            try
            {
                var result = await _repo.GetStates();

                if (result == null)
                {
                    return NotFound();
                }

                return result.ToList();
            }
            catch (Exception WTF)
            {
                // Log error.
                Console.WriteLine(WTF);
                return NotFound();
            }
        }

        // GET: api/PizzaTypes/sizes/5
        [HttpGet("{id}")]
        public async Task<ActionResult<State>> GetState(int id)
        {
            try
            {
                var state = await _repo.GetState(id);

                if (state == null)
                {
                    return NotFound();
                }

                return state;
            }
            catch (Exception WTF)
            {
                // Log error.
                Console.WriteLine(WTF);
                return NotFound();
            }
        }

        // GET: api/PizzaTypes/sizes/name/5
        [HttpGet("name/{id}")]
        public async Task<ActionResult<string>> GetStateName(int id)
        {
            try
            {
                var size = await _repo.GetStateName(id);

                if (size == null)
                {
                    return NotFound();
                }

                return size;
            }
            catch (Exception WTF)
            {
                // Log error.
                Console.WriteLine(WTF);
                return NotFound();
            }
        }

        // GET: api/PizzaTypes/sizes/price/5
        [HttpGet("abbr/{id}")]
        public async Task<ActionResult<string>> GetStateAbbrevation(int id)
        {
            try
            {
                var size = await _repo.GetStateAbbrevation(id);

                if (size == null)
                {
                    return NotFound();
                }

                return size;
            }
            catch (Exception WTF)
            {
                // Log error.
                Console.WriteLine(WTF);
                return NotFound();
            }
        }
    }
}