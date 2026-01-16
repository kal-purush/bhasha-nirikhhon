using System.Collections.Generic;
using System.Threading.Tasks;
using IP5.Model;
using IP5.Repositories;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace IP5.Controllers
{
	[ApiController]
	[Route("[controller]")]
	public class SessionPlayersController : ControllerBase
	{
		private readonly ISessionPlayersRepository _sessionPlayersRepository;
		private readonly ILogger<SessionPlayersController> _logger;

		public SessionPlayersController(ISessionPlayersRepository sessionPlayersRepository, ILogger<SessionPlayersController> logger)
		{
			_logger = logger;
			_sessionPlayersRepository = sessionPlayersRepository;
		}

		[HttpGet]
		public async IAsyncEnumerable<SessionPlayer> GetAll()
		{
			await foreach (var sessionPlayer in _sessionPlayersRepository.GetAll())
				yield return sessionPlayer;
		}

		[HttpPut]
		public void Create(SessionPlayer sessionPlayer)
		{
			_sessionPlayersRepository.Add(sessionPlayer);
		}
	}
}