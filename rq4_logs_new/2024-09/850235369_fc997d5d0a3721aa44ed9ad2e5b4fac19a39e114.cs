using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Bibliotheca.Data;
using Bibliotheca.Models;

namespace Bibliotheca.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AnimalObservationsController : ControllerBase
    {
        private readonly BibliothecaContext _context;

        public AnimalObservationsController(BibliothecaContext context)
        {
            _context = context;
        }

        // GET: api/AnimalObservations
        [HttpGet]
        public async Task<ActionResult<IEnumerable<AnimalObservation>>> GetAnimalObservation()
        {
            return await _context.AnimalObservation.ToListAsync();
        }

        // GET: api/AnimalObservations/5
        [HttpGet("{id}")]
        public async Task<ActionResult<AnimalObservation>> GetAnimalObservation(Guid id)
        {
            var animalObservation = await _context.AnimalObservation.FindAsync(id);

            if (animalObservation == null)
            {
                return NotFound();
            }

            return animalObservation;
        }

        // PUT: api/AnimalObservations/5
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPut("{id}")]
        public async Task<IActionResult> PutAnimalObservation(Guid id, AnimalObservation animalObservation)
        {
            if (id != animalObservation.Id)
            {
                return BadRequest();
            }

            _context.Entry(animalObservation).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!AnimalObservationExists(id))
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

        // POST: api/AnimalObservations
        // To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
        [HttpPost]
        public async Task<ActionResult<AnimalObservation>> PostAnimalObservation(AnimalObservation animalObservation)
        {
            _context.AnimalObservation.Add(animalObservation);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetAnimalObservation", new { id = animalObservation.Id }, animalObservation);
        }

        // DELETE: api/AnimalObservations/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteAnimalObservation(Guid id)
        {
            var animalObservation = await _context.AnimalObservation.FindAsync(id);
            if (animalObservation == null)
            {
                return NotFound();
            }

            _context.AnimalObservation.Remove(animalObservation);
            await _context.SaveChangesAsync();

            return NoContent();
        }

        private bool AnimalObservationExists(Guid id)
        {
            return _context.AnimalObservation.Any(e => e.Id == id);
        }
    }
}