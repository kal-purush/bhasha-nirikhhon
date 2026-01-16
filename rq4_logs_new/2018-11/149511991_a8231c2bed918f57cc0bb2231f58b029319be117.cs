using Api.Entity;
using Microsoft.AspNetCore.Mvc;
using Serilog;

namespace Api.Controllers.Base
{
	[ApiController]
	[Route("[controller]")]
	[Produces("application/json")]
	public abstract class ApiControllerBase : Controller
	{
		protected ILogger    Logger     { get; }
		protected ApiContext ApiContext { get; }

		protected ApiControllerBase(ILogger logger, ApiContext apiContext)
		{
			Logger     = logger;
			ApiContext = apiContext;
		}
	}
}