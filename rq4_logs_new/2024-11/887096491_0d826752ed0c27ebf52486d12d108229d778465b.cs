using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.Requests.Commands.LeaveManagement.LeaveTypes.AddLeaveType;

namespace WolfDen.API.Controllers.LeaveManagement.LeaveType
{
    [Route("api/leave-type")]
    [ApiController]
    public class LeaveType : ControllerBase
    {
        private readonly IMediator _mediator;

        public LeaveType(IMediator mediator)
        {
            _mediator = mediator;

        }
        [HttpPost]
        public async Task<bool> AddleaveType([FromBody] AddLeaveTypeCommand command)
        {
            return await _mediator.Send(command);
        }
    }
}