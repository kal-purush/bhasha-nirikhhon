using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MoviesApi.DTOs;
using MoviesApi.Models.Errors;
using System.Net;

namespace MoviesApi.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class GenresController : ControllerBase
	{
		private readonly ApplicationDbContext _context;

		public GenresController(ApplicationDbContext context)
		{
			_context = context;
		}

		[HttpGet]
		public async Task<IActionResult> GetAllAsync()
		{
			try
			{
				return Ok(await _context.Genres.ToListAsync());
			}
			catch
			{
				return BadRequest();
			}
		}

		[HttpPost]
		public async Task<IActionResult> CreateGenreAsync(GenreRequestDto genre)
		{
			if (genre == null)
			{
				var notFoundError = new ProblemDetails()
				{
					Status = (int)HttpStatusCode.NotFound,
					Title = "Not Found",
					Detail = "You should Specify the Genre Name"
				};
				return NotFound(notFoundError);
			}
			else
			{
				var g = new Genre { Name = genre.Name };
				await _context.AddAsync(g);
				_context.SaveChanges();

				return Ok(g);
			}
		}

		[HttpPut("{id}")]
		public async Task<IActionResult> UpdateAsync(int id, GenreRequestDto dto)
		{
			if(dto == null)
			{
				return BadRequest("The Genre Name should be defined !!");
			}

			var genre = await _context.Genres.FirstOrDefaultAsync(g => g.ID == id);
			if(genre is null)
			{
				return NotFound(new NotFoundError($"Cannot found Genre with ID : {id}"));
			}

			genre.Name = dto.Name;
			_context.SaveChanges();


			return Ok(genre);
		}


		[HttpDelete("{id}")]
		public async Task<IActionResult> DeleteAsync(int id)
		{
			var genre = await _context.Genres.FirstOrDefaultAsync(g => g.ID == id);
			if (genre is null)
			{
				return NotFound(new NotFoundError($"Cannot found Genre with ID : {id}"));
			}

			_context.Remove(genre);
			_context.SaveChanges();

			return Ok(genre);
		}
	}
}