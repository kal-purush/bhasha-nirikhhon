using System.Net.Mime;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;

namespace Mithrill.MonsterBook.WebApi.Controllers.Common
{
    [ApiController]
    [Produces(MediaTypeNames.Application.Json, "application/problem+json")]
    [ApiConventionType(typeof(CustomApiConventions))]
    [Route("api/[controller]")]
    public abstract class ApiControllerBase : ControllerBase
    {
        private ISender _mediator;

        protected ISender Mediator => _mediator ??= HttpContext.RequestServices.GetService<ISender>();
    }
}