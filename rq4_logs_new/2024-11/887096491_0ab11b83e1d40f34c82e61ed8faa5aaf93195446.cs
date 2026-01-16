using MediatR;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.DTOs.LeaveManagement;
using WolfDen.Application.Requests.Commands.LeaveManagement.LeaveSettings.UpdateLeaveSetting;
using WolfDen.Application.Requests.Commands.LeaveManagement.LeaveTypes.AddLeaveType;
using WolfDen.Application.Requests.Queries.LeaveManagement.LeaveSettings.GetLeaveSettings;

namespace WolfDen.API.Controllers.LeaveManagement
{
    [Route("api/leave-setting")]
    [ApiController]
    public class LeaveSettingController : ControllerBase
    {
        private readonly IMediator _mediatr;

        public LeaveSettingController(IMediator mediator)
        {
            _mediatr = mediator;
        }

        [HttpPut]
        public async Task<bool> UpdateLevaesetting([FromBody] UpdateLeaveSettingCommand command, CancellationToken cancellationToken)
        {
            return  await _mediatr.Send(command, cancellationToken);
           
        }

        [HttpGet]

        public async  Task<LeaveSettingDto> GetLeaveSetting(CancellationToken cancellationToken)
        {
            GetLeaveSettingQuery query = new GetLeaveSettingQuery();
            return await _mediatr.Send(query, cancellationToken);
        }
    }
}