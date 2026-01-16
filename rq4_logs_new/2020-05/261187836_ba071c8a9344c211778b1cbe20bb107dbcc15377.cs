using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using WebAppFpis.Models;

namespace WebAppFpis.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DrzavniOrganController : ControllerBase
    {
        private readonly MlekaraSaruljaContext _context;

        public DrzavniOrganController(MlekaraSaruljaContext context)
        {
            _context = context;
        }

        // GET: api/DrzavniOrgan
        [HttpGet]
        public IEnumerable<DrzavniOrgan> GetDrzavniOrgani()
        {
            return _context.DrzavniOrgani;
        }

        // GET: api/DrzavniOrgan/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetDrzavniOrgan([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var drzavniOrgan = await _context.DrzavniOrgani.FindAsync(id);

            if (drzavniOrgan == null)
            {
                return NotFound();
            }

            return Ok(drzavniOrgan);
        }

        // PUT: api/DrzavniOrgan/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutDrzavniOrgan([FromRoute] int id, [FromBody] DrzavniOrgan drzavniOrgan)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != drzavniOrgan.SifraOrganaID)
            {
                return BadRequest();
            }

            _context.Entry(drzavniOrgan).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!DrzavniOrganExists(id))
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

        // POST: api/DrzavniOrgan
        [HttpPost]
        public async Task<IActionResult> PostDrzavniOrgan([FromBody] DrzavniOrgan drzavniOrgan)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _context.DrzavniOrgani.Add(drzavniOrgan);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetDrzavniOrgan", new { id = drzavniOrgan.SifraOrganaID }, drzavniOrgan);
        }

        // DELETE: api/DrzavniOrgan/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteDrzavniOrgan([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var drzavniOrgan = await _context.DrzavniOrgani.FindAsync(id);
            if (drzavniOrgan == null)
            {
                return NotFound();
            }

            _context.DrzavniOrgani.Remove(drzavniOrgan);
            await _context.SaveChangesAsync();

            return Ok(drzavniOrgan);
        }

        private bool DrzavniOrganExists(int id)
        {
            return _context.DrzavniOrgani.Any(e => e.SifraOrganaID == id);
        }
    }
}