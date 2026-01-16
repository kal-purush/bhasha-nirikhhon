using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MinecraftSeeds.data;

namespace MinecraftSeeds.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SeedController : ControllerBase
    {
        private readonly MineCraftSeedContext _context;

        public SeedController(MineCraftSeedContext context)
        {
            _context = context;
        }

        // GET: api/Seed
        [HttpGet]
        public async Task<ActionResult<IEnumerable<seed>>> GetSeeds()
        {
            return await _context.Seeds.ToListAsync();
        }

        // GET: api/Seed/5
        [HttpGet("{id}")]
        public async Task<ActionResult<seed>> Getseed(int id)
        {
            var seed = await _context.Seeds.FindAsync(id);

            if (seed == null)
            {
                return NotFound();
            }

            return seed;
        }

        // PUT: api/Seed/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Putseed(int id, seed seed)
        {
            if (id != seed.SeedID)
            {
                return BadRequest();
            }

            _context.Entry(seed).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!seedExists(id))
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

        // POST: api/Seed
        [HttpPost]
        public async Task<ActionResult<seed>> Postseed(seed seed)
        {
            _context.Seeds.Add(seed);
            await _context.SaveChangesAsync();

            return CreatedAtAction("Getseed", new { id = seed.SeedID }, seed);
        }

        // DELETE: api/Seed/5
        [HttpDelete("{id}")]
        public async Task<ActionResult<seed>> Deleteseed(int id)
        {
            var seed = await _context.Seeds.FindAsync(id);
            if (seed == null)
            {
                return NotFound();
            }

            _context.Seeds.Remove(seed);
            await _context.SaveChangesAsync();

            return seed;
        }

        private bool seedExists(int id)
        {
            return _context.Seeds.Any(e => e.SeedID == id);
        }
    }
}