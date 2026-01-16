#nullable disable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Mybnb.api.Data;
using Mybnb.api.Models;
//Add Tenant and renting periods to this controller and delete the others
namespace Mybnb.api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BNBsController : ControllerBase
    {
        private readonly MybnbapiContext _context;

        public BNBsController(MybnbapiContext context)
        {
            _context = context;
        }

        // GET: api/BNBs
        [HttpGet]
        public async Task<ActionResult<IEnumerable<BNB>>> GetBNB()
        {
            return await _context.BNBs.ToListAsync();
        }

        // GET: api/BNBs/5
        [HttpGet("{id}")]
        public async Task<ActionResult<BNB>> GetBNB(int id)
        {
            var bNB = await _context.BNBs.FindAsync(id);

            if (bNB == null)
            {
                return NotFound();
            }

            return bNB;
        }

        // PUT: api/BNBs/5
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPut("{id}")]
        public async Task<IActionResult> PutBNB(int id, BNB bNB)
        {
            if (id != bNB.ID)
            {
                return BadRequest();
            }

            _context.Entry(bNB).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!BNBExists(id))
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

        // POST: api/BNBs
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPost]
        public async Task<ActionResult<BNB>> PostBNB(BNB bNB)
        {
            _context.BNBs.Add(bNB);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetBNB", new { id = bNB.ID }, bNB);
        }

        // DELETE: api/BNBs/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteBNB(int id)
        {
            var bNB = await _context.BNBs.FindAsync(id);
            if (bNB == null)
            {
                return NotFound();
            }

            _context.BNBs.Remove(bNB);
            await _context.SaveChangesAsync();

            return NoContent();
        }

        private bool BNBExists(int id)
        {
            return _context.BNBs.Any(e => e.ID == id);
        }
    }
}