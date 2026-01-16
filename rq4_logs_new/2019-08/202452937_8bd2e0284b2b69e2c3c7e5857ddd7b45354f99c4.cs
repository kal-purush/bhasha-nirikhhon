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
    public class CloracionController : ControllerBase
    {
        private readonly AsadaContext _context;

        public CloracionController(AsadaContext context)
        {
            _context = context;
        }

        // GET: api/Cloracion
        [HttpGet]
        public IEnumerable<Cloracion> GetCloracion()
        {
            return _context.Cloracion;
        }

        // GET: api/Cloracion/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetCloracion([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var cloracion = await _context.Cloracion.FindAsync(id);

            if (cloracion == null)
            {
                return NotFound();
            }

            return Ok(cloracion);
        }

        // PUT: api/Cloracion/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutCloracion([FromRoute] int id, [FromBody] Cloracion cloracion)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (id != cloracion.Id)
            {
                return BadRequest();
            }

            _context.Entry(cloracion).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!CloracionExists(id))
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

        // POST: api/Cloracion
        [HttpPost]
        public async Task<IActionResult> PostCloracion([FromBody] Cloracion cloracion)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _context.Cloracion.Add(cloracion);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetCloracion", new { id = cloracion.Id }, cloracion);
        }

        // DELETE: api/Cloracion/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteCloracion([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var cloracion = await _context.Cloracion.FindAsync(id);
            if (cloracion == null)
            {
                return NotFound();
            }

            _context.Cloracion.Remove(cloracion);
            await _context.SaveChangesAsync();

            return Ok(cloracion);
        }

        private bool CloracionExists(int id)
        {
            return _context.Cloracion.Any(e => e.Id == id);
        }
    }
}