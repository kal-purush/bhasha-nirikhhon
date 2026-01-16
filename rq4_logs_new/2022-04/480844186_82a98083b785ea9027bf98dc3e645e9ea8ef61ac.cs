using Microsoft.AspNetCore.Mvc;
using Sample.CQRS.Api.Domain.Commands.Requests;
using Sample.CQRS.Api.Domain.Handlers;
using Sample.CQRS.Api.Domain.Queries.Requests;

namespace Sample.CQRS.Api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ProductController : ControllerBase
    {
        [HttpPost]
        [Route("")]
        public IActionResult Create(
            [FromServices] ICreateProductHandler handler,
            [FromBody] CreateProductRequest request
        )
        {
            var response = handler.Handle(request);

            return Ok(response);
        }

        [HttpGet]
        [Route("")]
        public IActionResult GetById(
            [FromServices] IFindProductByIdHandler handler,
            [FromQuery] FindProductByIdRequest command
        )
        {
            var result = handler.Handle(command);
            return Ok(result);
        }
    }
}