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
	public class SessionsController : ControllerBase
	{
		private readonly ISessionsRepository _sessionRepository;
		private readonly ILogger<SessionsController> _logger;

		public SessionsController(ISessionsRepository sessionRepository, ILogger<SessionsController> logger)
		{
			_logger = logger;
			_sessionRepository = sessionRepository;
		}

		[HttpGet]
		public async IAsyncEnumerable<Session> GetAll()
		{
			await foreach (var session in _sessionRepository.GetAll())
				yield return session;
		}

		[HttpGet("{code}")]
		public Task<Session> GetByCode([FromRoute] string code)
		{
			return _sessionRepository.Get(code);
		}

	}
}