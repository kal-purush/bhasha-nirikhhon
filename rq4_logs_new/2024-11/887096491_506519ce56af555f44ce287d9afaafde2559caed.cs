using MediatR;
using Microsoft.AspNetCore.Mvc;
using WolfDen.Application.DTOs.Attendence;
using WolfDen.Application.Requests.Commands.Attendence.SendNotification;
using WolfDen.Application.Requests.Queries.Attendence.DailyAttendanceReport;
using WolfDen.Application.Requests.Queries.Notifications.GetNotification;

namespace WolfDen.API.Controllers.Attendence
{
    [Route("api/[controller]")]
    [ApiController]
    public class NotificationController : ControllerBase
    {
        private readonly IMediator _mediator;
        public NotificationController(IMediator mediator, PdfService pdfService)
        {
            _mediator = mediator;
        }

        [HttpPost("notification")]
        public async Task<int> SendNotification([FromBody] NotificationCommand command, CancellationToken cancellationToken)
        {
            return await _mediator.Send(command, cancellationToken);
        }

        [HttpGet("employee")]
        public async Task<List<NotificationDTO>> GetMonthlyAttendance([FromQuery] GetNotificationQuery query, CancellationToken cancellationToken)
        {
            return await _mediator.Send(query, cancellationToken);
        }
    }
}