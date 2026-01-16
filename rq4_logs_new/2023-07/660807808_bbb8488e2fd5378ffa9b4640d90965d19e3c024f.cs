using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using System.Net;

namespace MoviesApi.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class MoviesController : ControllerBase
	{
		private readonly ApplicationDbContext _context;

		public MoviesController(ApplicationDbContext context)
		{
			_context = context;
		}

		[HttpPost]
		public async Task<IActionResult> Create([FromForm] MovieRequestDto dto)
		{
			var errors = new List<string>();
			await CreateMovieErrorsHandler(dto, errors);

			if (errors.Count > 0)
			{
				return HandleBadRequestError(errors);
			}

			using var dataStream = new MemoryStream();
			await dto.Poster!.CopyToAsync(dataStream);
			var movie = new Movie
			{
				GenreID = dto.GenreID!.Value,
				Poster = dataStream.ToArray(),
				Rate = dto.Rate!.Value,
				StoryLine = dto.StoryLine!,
				Title = dto.Title,
				Year = dto.Year!.Value,
			};
			_context.Add(movie);
			await _context.SaveChangesAsync();

			return SuccessObjectResult(movie, (int)HttpStatusCode.Created);
		}

		private static ObjectResult SuccessObjectResult(Movie movie, int statusCode)
		{
			return new ObjectResult(new CustomResponse<Movie>()
			{
				StatusCode = statusCode,
				Data = movie,
				Message = "Success !!",
			})
			{ StatusCode = statusCode };
		}

		private async Task CreateMovieErrorsHandler(MovieRequestDto dto, List<string> errors)
		{
			if(dto is null)
			{
				errors.Add("Please Provide you Movie info");
				return;
			}
			var isValidGenre = await _context.Genres.AnyAsync(G => G.ID == dto.GenreID);
			if (dto.Poster is null)
			{
				errors.Add(HandleErrorMessage("Poster"));
			}

			if (dto.Title.IsNullOrEmpty())
			{
				errors.Add(HandleErrorMessage("Title"));
			}

			if(dto.Rate is null)
			{
				errors.Add(HandleErrorMessage("Rate"));
			}else if(dto.Rate.Value <0 || dto.Rate.Value > 10)
			{
				errors.Add("Invalid Rate , the Rate should be between 0 - 10");
			}

			if(dto.GenreID is null)
			{
				errors.Add("You should provide GenreId for the Movie");
			}
			else if (dto.GenreID <= 0)
			{
				errors.Add("The Genre ID should be more than 0 ");
			}
			else if (!isValidGenre)
			{
				errors.Add($"The Provided Genre ID : {dto.GenreID} doesn't exists !!");
			}
		}

		private string HandleErrorMessage(string name)
		{
			return $"You should send {name}";
		}

		private static ObjectResult HandleBadRequestError(List<string> errors)
		{
			return new BadRequestObjectResult(new CustomResponse<object>
			{
				Message = "You response was bad , see the errors !!!",
				Status = false,
				StatusCode = (int)HttpStatusCode.BadRequest,
				Errors = errors
			});
		}
	}


}