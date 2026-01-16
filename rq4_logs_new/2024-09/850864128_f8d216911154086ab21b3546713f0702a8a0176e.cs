using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Hosting;
using PokeDexter.Components.Models;
using System.Text.Json;

namespace PokeDexter.Components.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class FavoritesController : ControllerBase
    {
        [HttpPost("save")]
        public async Task<IActionResult> SaveFavorites([FromBody] List<Pokemon> favorites)
        {
            Console.WriteLine("SaveFavorites called"); 

            if (favorites == null || !favorites.Any())
            {
                return BadRequest(new { message = "Favorites cannot be null or empty." });
            }

            var json = JsonSerializer.Serialize(favorites);
            var filePath = Path.Combine(Directory.GetCurrentDirectory(), "Favorites.json");

            await System.IO.File.WriteAllTextAsync(filePath, json);
            return Ok(new { message = "Favorites saved successfully." });
        }
    }
}