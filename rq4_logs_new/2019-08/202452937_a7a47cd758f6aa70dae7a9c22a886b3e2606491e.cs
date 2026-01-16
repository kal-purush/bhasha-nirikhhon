using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using API_Asada.Models;

namespace API_Asada.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AforoController : ControllerBase
    {
        private readonly AsadaContext _context;

        public AforoController(AsadaContext context)
        {
            _context = context;
        }

        // GET: api/Aforo
        [HttpGet]
        public IEnumerable<Aforo> GetAforo()
        {
            return _context.Aforo;
        }

        // GET: api/Aforo/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetAforo([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var aforo = await _context.Aforo.FindAsync(id);

            if (aforo == null)
            {
                return NotFound();
            }

            return Ok(aforo);
        }

        // PUT: api/Aforo/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutAforo([FromRoute] int id, [FromBody] Aforo aforo)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != aforo.Id)
            {
                return BadRequest();
            }

            _context.Entry(aforo).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!AforoExists(id))
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

        // POST: api/Aforo
        [HttpPost]
        public async Task<IActionResult> PostAforo([FromBody] Aforo aforo)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _context.Aforo.Add(aforo);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetAforo", new { id = aforo.Id }, aforo);
        }

        // DELETE: api/Aforo/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteAforo([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var aforo = await _context.Aforo.FindAsync(id);
            if (aforo == null)
            {
                return NotFound();
            }

            _context.Aforo.Remove(aforo);
            await _context.SaveChangesAsync();

            return Ok(aforo);
        }

        private bool AforoExists(int id)
        {
            return _context.Aforo.Any(e => e.Id == id);
        }
    }
}