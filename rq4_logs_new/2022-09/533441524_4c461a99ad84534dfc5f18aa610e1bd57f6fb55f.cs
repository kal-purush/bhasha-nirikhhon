using Common.Helpers;
using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace APIContract.Controllers.Base
{
    public class HttpController : ControllerBase
    {
        protected readonly ILogger<HttpController> _logger;
        protected readonly IMediator _mediator;

        protected HttpController(
            ILogger<HttpController> logger,
            IMediator mediator)
        {
            _logger = logger;
            _mediator = mediator;
        }

        protected async Task<ActionResult> Created(
            object command,
            string actionName)
        {
            var result = await _mediator.Send(command);

            var status = StatusCodes.Status201Created;
            var response = ResponseFactory<object>.SuccessResponse(status, result);

            return CreatedAtAction(actionName, new
            {
                Id = result.GetType().GetProperty("Id")
            },
            response);
        }

        protected new async Task<ActionResult> Response(object command)
        {
            var result = await _mediator.Send(command);

            var status = StatusCodes.Status200OK;
            var response = ResponseFactory<object>.SuccessResponse(status, result);

            return new OkObjectResult(response);
        }
    }
}