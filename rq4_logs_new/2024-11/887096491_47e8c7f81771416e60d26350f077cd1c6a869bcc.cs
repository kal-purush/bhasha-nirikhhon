using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.DTOs.LeaveManagement;
using WolfDen.Application.Requests.Queries.LeaveManagement.LeaveRequests.GetLeaveRequestHistory;

namespace WolfDen.API.Controllers.LeaveManagement
{
    [Route("api/leave-request")]
    [ApiController]
    public class LeaveRequestController : ControllerBase
    {
        private readonly IMediator _mediator;

        public LeaveRequestController(IMediator mediator)
        {
            _mediator=mediator;
        }

        [HttpGet]
        public async Task<List<LeaveRequestDto>> GetLeaveRequestHistory(int requestId)
        {
            GetLeaveRequestHistoryQuery query= new GetLeaveRequestHistoryQuery();
            query.RequestId=requestId;
            return await _mediator.Send(query); 
        }
    }
}