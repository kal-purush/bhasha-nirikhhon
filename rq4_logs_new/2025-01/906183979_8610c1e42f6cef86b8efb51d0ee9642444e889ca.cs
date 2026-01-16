using Application.Commands.CVCommands;
using Domain.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace MyCvSite.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CVController : ControllerBase
    {
        private readonly IMediator _mediator;

        public CVController(IMediator mediator)
        {
            _mediator = mediator;
        }

        [HttpPost]
        public async Task<IActionResult> Create([FromBody] CV cv)
        {
            var addNewAuthorOperationResult = await _mediator.Send(new CreateCVCommand(cv));
            return Ok();
        }

        [HttpGet]
        public async Task<IActionResult> GetAllCvs()
        {
            return Ok();
        }

        [HttpGet("{id:guid}")]
        public async Task<IActionResult> GetCvById(Guid id, CancellationToken cancellationToken)
        {
            return Ok();
        }

        [HttpPut("{id:guid}")]
        public async Task<IActionResult> UpdateCvById(Guid id, [FromBody] CV cv, CancellationToken cancellationToken)
        {
            return Ok();
        }

        [HttpDelete("{id:guid}")]
        public async Task<IActionResult> DeleteCvById(Guid id)
        {
          return Ok();
        }

    }
}