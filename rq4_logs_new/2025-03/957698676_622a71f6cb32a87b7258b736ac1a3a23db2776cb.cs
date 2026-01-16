using System.Net.Mime;
using System.Threading;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Soenneker.Controllers.Base;
using Soenneker.Enums.DeployEnvironment;

namespace Soenneker.Controllers.Health;

/// <summary>
/// Provides a health check endpoint to verify if the service is online. Returns a message indicating the API status
/// based on the environment.
/// </summary>
[ApiController]
[Route("")]
public sealed class HealthController : BaseController
{
    public HealthController(IConfiguration config) : base(config)
    {
    }

    /// <summary>
    /// Checks the health status of the API and returns a response indicating whether it is online.
    /// </summary>
    /// <param name="cancellationToken">Used to signal cancellation of the operation if needed.</param>
    /// <returns>Returns a 200 OK response with a health message if in development or local environment; otherwise, returns a 200
    /// OK without a message.</returns>
    [HttpGet]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [Produces(MediaTypeNames.Application.Json)]
    [Consumes(MediaTypeNames.Application.Json)]
    [AllowAnonymous]
    public IActionResult Health(CancellationToken cancellationToken)
    {
        string? environment = Config["Environment"];

        if (environment != DeployEnvironment.Local && environment != DeployEnvironment.Development)
            return Ok();

        const string health = "API is online";
        return Ok(health);
    }
}