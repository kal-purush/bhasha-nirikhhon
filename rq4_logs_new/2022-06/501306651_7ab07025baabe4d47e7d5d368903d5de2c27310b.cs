using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Finanzas.Data;
using Finanzas.Model;

namespace Finanzas.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CategoriasApiController : ControllerBase
    {
        private readonly AppDbContext _context;

        public CategoriasApiController(AppDbContext context)
        {
            _context = context;
        }

        // GET: api/CategoriasApi
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Categoria>>> Getcategoria()
        {
          if (_context.categoria == null)
          {
              return NotFound();
          }
            return await _context.categoria.ToListAsync();
        }

        // GET: api/CategoriasApi/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Categoria>> GetCategoria(int id)
        {
          if (_context.categoria == null)
          {
              return NotFound();
          }
            var categoria = await _context.categoria.FindAsync(id);

            if (categoria == null)
            {
                return NotFound();
            }

            return categoria;
        }

        // PUT: api/CategoriasApi/5
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPut("{id}")]
        public async Task<IActionResult> PutCategoria(int id, Categoria categoria)
        {
            if (id != categoria.Id)
            {
                return BadRequest();
            }

            _context.Entry(categoria).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!CategoriaExists(id))
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

        // POST: api/CategoriasApi
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPost]
        public async Task<ActionResult<Categoria>> PostCategoria(Categoria categoria)
        {
          if (_context.categoria == null)
          {
              return Problem("Entity set 'AppDbContext.categoria'  is null.");
          }
            _context.categoria.Add(categoria);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetCategoria", new { id = categoria.Id }, categoria);
        }

        // DELETE: api/CategoriasApi/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteCategoria(int id)
        {
            if (_context.categoria == null)
            {
                return NotFound();
            }
            var categoria = await _context.categoria.FindAsync(id);
            if (categoria == null)
            {
                return NotFound();
            }

            _context.categoria.Remove(categoria);
            await _context.SaveChangesAsync();

            return NoContent();
        }

        private bool CategoriaExists(int id)
        {
            return (_context.categoria?.Any(e => e.Id == id)).GetValueOrDefault();
        }
    }
}