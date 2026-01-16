using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OdeToFood.Core;
using OdeToFood.Data;

namespace OdeToFood.Api
{
    [Route("api/[controller]")]
    [ApiController]
    public class RestaurantsController : ControllerBase
    {
        private readonly OdeToFoodDbContext _context;

        public RestaurantsController(OdeToFoodDbContext context)
        {
            _context = context;
        }

        // GET: api/Restaurants
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Restaurants>>> GetRestaurants()
        {
            return await _context.Restaurants.ToListAsync();
        }

        // GET: api/Restaurants/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Restaurants>> GetRestaurants(int id)
        {
            var restaurants = await _context.Restaurants.FindAsync(id);

            if (restaurants == null)
            {
                return NotFound();
            }

            return restaurants;
        }

        // PUT: api/Restaurants/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for
        // more details see https://aka.ms/RazorPagesCRUD.
        [HttpPut("{id}")]
        public async Task<IActionResult> PutRestaurants(int id, Restaurants restaurants)
        {
            if (id != restaurants.Id)
            {
                return BadRequest();
            }

            _context.Entry(restaurants).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!RestaurantsExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }

            return NoContent();
        }

        // POST: api/Restaurants
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for
        // more details see https://aka.ms/RazorPagesCRUD.
        [HttpPost]
        public async Task<ActionResult<Restaurants>> PostRestaurants(Restaurants restaurants)
        {
            _context.Restaurants.Add(restaurants);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetRestaurants", new { id = restaurants.Id }, restaurants);
        }

        // DELETE: api/Restaurants/5
        [HttpDelete("{id}")]
        public async Task<ActionResult<Restaurants>> DeleteRestaurants(int id)
        {
            var restaurants = await _context.Restaurants.FindAsync(id);
            if (restaurants == null)
            {
                return NotFound();
            }

            _context.Restaurants.Remove(restaurants);
            await _context.SaveChangesAsync();

            return restaurants;
        }

        private bool RestaurantsExists(int id)
        {
            return _context.Restaurants.Any(e => e.Id == id);
        }
    }
}