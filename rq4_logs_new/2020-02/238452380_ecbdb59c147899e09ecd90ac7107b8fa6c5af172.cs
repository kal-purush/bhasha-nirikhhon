using System.Collections.Generic;
using IP5.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace IP5.Controllers
{
	[ApiController]
	[Route("[controller]")]
	public class GamesController : ControllerBase
	{
		private readonly ILogger<GamesController> _logger;

		public GamesController(ILogger<GamesController> logger)
		{
			_logger = logger;
		}

		[HttpGet]
		public IEnumerable<Game> Get()
		{
			return new[] {
				new Game {Player1 = "Joel", Player2 = "Rohn", Score1 = 3, Score2 = 6, Ongoing = true},
				new Game {Player1 = "Jan", Player2 = "Bram", Score1 = 5, Score2 = 11},
				new Game {Player1 = "Andy", Player2 = "Raul", Score1 = 11, Score2 = 9},
			};
		}
	}
}