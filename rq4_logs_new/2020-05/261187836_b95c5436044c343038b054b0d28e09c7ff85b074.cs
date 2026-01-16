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
    public class MestoController : ControllerBase
    {
        private readonly SaruljaContext _context;

        public MestoController(SaruljaContext context)
        {
            _context = context;
        }

        // GET: api/Mesto
        [HttpGet]
        public IEnumerable<Mesto> GetMesta()
        {
            return _context.Mesta;
        }

        // GET: api/Mesto/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetMesto([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var mesto = await _context.Mesta.FindAsync(id);

            if (mesto == null)
            {
                return NotFound();
            }

            return Ok(mesto);
        }

        // PUT: api/Mesto/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutMesto([FromRoute] int id, [FromBody] Mesto mesto)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != mesto.MestoID)
            {
                return BadRequest();
            }

            _context.Entry(mesto).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!MestoExists(id))
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

        // POST: api/Mesto
        [HttpPost]
        public async Task<IActionResult> PostMesto([FromBody] Mesto mesto)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _context.Mesta.Add(mesto);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetMesto", new { id = mesto.MestoID }, mesto);
        }

        // DELETE: api/Mesto/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteMesto([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var mesto = await _context.Mesta.FindAsync(id);
            if (mesto == null)
            {
                return NotFound();
            }

            _context.Mesta.Remove(mesto);
            await _context.SaveChangesAsync();

            return Ok(mesto);
        }

        private bool MestoExists(int id)
        {
            return _context.Mesta.Any(e => e.MestoID == id);
        }
    }
}