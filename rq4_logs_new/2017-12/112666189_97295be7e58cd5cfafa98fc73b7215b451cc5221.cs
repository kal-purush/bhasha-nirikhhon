using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ZenithSocietyA2.Data;
using ZenithSocietyA2.Models;
using ZenithSocietyA2.Data;

namespace ZenithSocietyA2.Controllers
{
    [Produces("application/json")]
    [Route("api/ActivityCategories")]
    public class ActivityCategoriesapiController : Controller
    {
        private readonly ApplicationDbContext _context;

        public ActivityCategoriesapiController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: api/Studentsapi
        [HttpGet]
        public IEnumerable<ActivityCategory> GetActivityCategories()
        {
            return _context.ActivityCategories;
        }

        // GET: api/ActivityCategoriesapi/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetActivityCategory([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var @event = await _context.ActivityCategories.SingleOrDefaultAsync(m => m.ActivityCategoryId == id);

            if (@event == null)
            {
                return NotFound();
            }

            return Ok(@event);
        }

        // PUT: api/Studentsapi/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutActivityCategory([FromRoute] int id, [FromBody] ActivityCategory @event)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != @event.ActivityCategoryId)
            {
                return BadRequest();
            }

            _context.Entry(@event).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!StudentExists(id))
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

        // POST: api/Studentsapi
        [HttpPost]
        public async Task<IActionResult> PostActivityCategory([FromBody] ActivityCategory @event)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _context.ActivityCategories.Add(@event);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetActivityCategory", new { id = @event.ActivityCategoryId }, @event);
        }

        // DELETE: api/Studentsapi/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteStudent([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var @event = await _context.ActivityCategories.SingleOrDefaultAsync(m => m.ActivityCategoryId == id);
            if (@event == null)
            {
                return NotFound();
            }

            _context.ActivityCategories.Remove(@event);
            await _context.SaveChangesAsync();

            return Ok(@event);
        }

        private bool StudentExists(int id)
        {
            return _context.ActivityCategories.Any(e => e.ActivityCategoryId == id);
        }
    }
}