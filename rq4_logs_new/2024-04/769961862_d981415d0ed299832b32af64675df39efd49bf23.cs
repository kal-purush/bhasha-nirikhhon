using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using App.Data;
using App.Models; 

namespace App.Controllers
{
    [Route("api/animals")]
    [ApiController]
    public class AnimalsController : ControllerBase
    {
        private readonly ApplicationDbContext _context;

        public AnimalsController(ApplicationDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<IActionResult> GetAnimals([FromQuery] string orderBy = "name")
        {
            IQueryable<Animal> animalsQuery = _context.Animals;

            switch (orderBy.ToLowerInvariant())
            {
                case "name":
                    animalsQuery = animalsQuery.OrderBy(a => a.Name);
                    break;
                case "description":
                    animalsQuery = animalsQuery.OrderBy(a => a.Description);
                    break;
                case "category":
                    animalsQuery = animalsQuery.OrderBy(a => a.Category);
                    break;
                case "area":
                    animalsQuery = animalsQuery.OrderBy(a => a.Area);
                    break;
                default:
                    return BadRequest("Invalid order by parameter");
            }

            var animals = await animalsQuery.ToListAsync();
            return Ok(animals);
        }

        [HttpPost]
        public async Task<IActionResult> AddAnimal([FromBody] Animal animal)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _context.Animals.Add(animal);
            await _context.SaveChangesAsync();

            return CreatedAtAction(nameof(GetAnimals), new { id = animal.IdAnimal }, animal);
        }

        [HttpPut("{idAnimal}")]
        public async Task<IActionResult> UpdateAnimal(int idAnimal, [FromBody] Animal animal)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            if (idAnimal != animal.IdAnimal)
            {
                return BadRequest("ID mismatch");
            }

            _context.Entry(animal).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!_context.Animals.Any(a => a.IdAnimal == idAnimal))
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

        [HttpDelete("{idAnimal}")]
        public async Task<IActionResult> DeleteAnimal(int idAnimal)
        {
            var animal = await _context.Animals.FindAsync(idAnimal);
            if (animal == null)
            {
                return NotFound();
            }

            _context.Animals.Remove(animal);
            await _context.SaveChangesAsync();

            return NoContent();
        }
    }
}