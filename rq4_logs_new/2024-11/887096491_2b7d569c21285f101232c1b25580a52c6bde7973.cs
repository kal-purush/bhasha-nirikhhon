using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.Requests.Commands.LeaveTypeCommand;

namespace WolfDen.API.Controllers
{
    [Route("api/LeaveType")]
    [ApiController]
    public class LeaveTypeController : ControllerBase
    {
        private readonly IMediator _mediator;

        public LeaveTypeController(IMediator mediator)
        {
            _mediator = mediator;
            
        }
        [HttpPost("Add-New-Type")]
        public async Task<string> AddleaveType([FromBody] LeaveTypeAddCommand command)
        {
            int result = await _mediator.Send(command);
            if (result == -1)
            {
                return ("Only Higher User can Add new leave Type");
            }
            else if(result == 1)
            {
                return ("New Leave Added");
            }
            else
            {
                return ("Error");
            }

        }
    }
}