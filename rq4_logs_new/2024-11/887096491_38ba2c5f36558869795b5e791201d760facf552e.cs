using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.Requests.Commands.DeparmentCommands;

namespace WolfDen.API.Controllers.DepartmentController
{
    [Route("api/[controller]")]
    [ApiController]
    public class DepartmentController : ControllerBase
    {
        private readonly IMediator _mediator;

        public DepartmentController(IMediator mediator)
        {
            _mediator = mediator;
        }
        [HttpPost]
        public async Task<int> AddDepartment([FromBody]AddDepartmentCommand command)
        {
            return await _mediator.Send(command);
        }
    }
}