using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Cafeteros.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Http;

namespace Cafeteros.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AspectoEconomicoController: ControllerBase
    {
        private readonly ApplicationDbContext _context;

        public AspectoEconomicoController(ApplicationDbContext context){
            _context = context;
            if (_context.AspectoEconomico.Count() == 0){
                _context.AspectoEconomico.Add(new AspectoEconomico { TenenciaTierra = "312 ", Legalidad = "12334", ProductorId = "1"});
                _context.AspectoEconomico.Add(new AspectoEconomico { TenenciaTierra = "12323", Legalidad = "Dia1212z", ProductorId = "2"});
                _context.SaveChanges();
            }
        }
   
        [HttpGet]
        public async Task<ActionResult<IEnumerable<AspectoEconomico>>> GetAspectoEconomicos()
        {
            return await _context.AspectoEconomico.ToListAsync();
        }

        // GET: api/Task/5
        [HttpGet("{id}")]
        public async Task<ActionResult<AspectoEconomico>> GetAspectoEconomico(int id)
        {
            var AspectoEconomico = await _context.AspectoEconomico.FindAsync(id);
            if (AspectoEconomico == null){
                return NotFound();
            }
            return AspectoEconomico;
        }

        // POST: api/Task
        [HttpPost]
        public async Task<ActionResult<AspectoEconomico>> PostAspectoEconomico(AspectoEconomico item)
        {
            _context.AspectoEconomico.Add(item);
            await _context.SaveChangesAsync();
            return CreatedAtAction(nameof(GetAspectoEconomico), new { id = item.id }, item);
        }

        // PUT: api/Task/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutAspectoEconomico(int id, AspectoEconomico item)
        {
            if (id != item.id)
            {
            return BadRequest();
            }
            _context.Entry(item).State = EntityState.Modified;
            await _context.SaveChangesAsync();
            return NoContent();
        }

        // DELETE: api/Task/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var AspectoEconomico = await _context.AspectoEconomico.FindAsync(id);

            if (AspectoEconomico == null)
            {
                return NotFound();
            }

            _context.AspectoEconomico.Remove(AspectoEconomico);
            await _context.SaveChangesAsync();

            return NoContent();
        }

        
    }
}