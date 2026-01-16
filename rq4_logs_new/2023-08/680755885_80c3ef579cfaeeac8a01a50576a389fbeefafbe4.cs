using System;
using System.Threading.Tasks;
using AutoMapper;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using TestTemplate2.Api.Helpers;
using TestTemplate2.Application.Questions.Commands;
using TestTemplate2.Application.Questions.Queries;

namespace TestTemplate2.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class FoosController : ControllerBase
    {
        private readonly ISender _sender;
        private readonly IMapper _mapper;

        public FoosController(
            ISender sender,
            IMapper mapper)
        {
            _sender = sender;
            _mapper = mapper;
        }

        /// <summary>
        /// Get a single foo.
        /// </summary>
        /// <param name="getFooQuery">Specifies which foo to fetch.</param>
        /// <returns>Foo data.</returns>
        [ProducesResponseType(StatusCodes.Status200OK)]
        [Produces("application/json")]
        [HttpGet("{id}", Name = "GetFoo")]
        public async Task<ActionResult<FooGetModel>> GetAsync([FromRoute] GetFooQuery getFooQuery)
        {
            var foo = await _sender.Send(getFooQuery);
            var response = _mapper.Map<FooGetModel>(foo);
            return Ok(response);
        }

        /// <summary>
        /// Create a new foo.
        /// </summary>
        /// <param name="createFooCommand">Foo create body.</param>
        /// <returns>Newly created foo.</returns>
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(typeof(ValidationProblemDetails), StatusCodes.Status422UnprocessableEntity)]
        [Produces("application/json")]
        [Consumes("application/json")]
        //[Authorize]
        [HttpPost]
        public async Task<ActionResult<FooGetModel>> PostAsync([FromBody] CreateFooCommand createFooCommand)
        {
            var foo = await _sender.Send(createFooCommand);
            var response = _mapper.Map<FooGetModel>(foo);
            return CreatedAtRoute("GetFoo", new { id = foo.Id }, response);
        }

        /// <summary>
        /// Edit foo.
        /// </summary>
        /// <param name="id">Foo identifier.</param>
        /// <param name="updateFooCommand">Foo edit data.</param>
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [Produces("application/json")]
        [Consumes("application/json")]
        //[Authorize]
        [HttpPut("{id}")]
        public async Task<ActionResult> PutAsync([FromRoute]Guid id, [FromBody] UpdateFooCommand updateFooCommand)
        {
            await _sender.Send(updateFooCommand);
            return NoContent();
        }

        /// <summary>
        /// Delete foo.
        /// </summary>
        /// <param name="id">Foo identifier.</param>
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        [Produces("application/json")]
        //[Authorize]
        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteAsync(Guid id)
        {
            var deleteQuestionCommand = new DeleteFooCommand
            {
                Id = id
            };
            await _sender.Send(deleteQuestionCommand);
            return NoContent();
        }
    }
}