using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.DTOs.LeaveManagement;
using WolfDen.Application.Requests.Queries.LeaveManagement.LeaveRequestDays;

namespace WolfDen.API.Controllers.LeaveManagement
{
    [Route("api/[controller]")]
    [ApiController]
    public class LeaveRequestDayController(IMediator mediator) : ControllerBase
    {
        private readonly IMediator _mediator = mediator;

        [HttpGet("leaves-on-current-day")]
        public async Task<List<PresentDayApprovedLeavesDto>> GetLeaveOnPresent()
        {
            GetAllApprovedLeavesTodayQuery query = new GetAllApprovedLeavesTodayQuery();
            return await _mediator.Send(query);
        }
    }
}