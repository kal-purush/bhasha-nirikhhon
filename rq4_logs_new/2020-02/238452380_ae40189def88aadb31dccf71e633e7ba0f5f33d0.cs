using System.Collections.Generic;
using IP5.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Linq;
using System;

namespace IP5.Controllers
{
	[ApiController]
	[Route("[controller]")]
	public class PlayersController : ControllerBase
	{
		private readonly ILogger<PlayersController> _logger;

		public PlayersController(ILogger<PlayersController> logger)
		{
			_logger = logger;
		}

		[HttpGet("{code?}")]
		public IEnumerable<Player> Get(string code)
		{
			var data = getData();

			var list = data.ToList();

			if (!String.IsNullOrEmpty(code)) {
				return new[] { list.Find((item) => item.Code == code) };
			}

			return data;
		}

		private IEnumerable<Player> getData() {
			return new[] {
				new Player {Code = "jl" , Description = "Joel", Wins = 3, Losses = 6},
				new Player {Code = "jan", Description = "Jan", Wins = 5, Losses = 11, IsChampion = true},
				new Player {Code = "andy" , Description = "Andy", Wins = 11, Losses = 9},
			};
		} 
	}
}


