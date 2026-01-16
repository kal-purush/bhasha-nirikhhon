using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

using System.Threading.Tasks;
using System.Collections.Generic;

using Application.Notifications.Queries.GetCurrentUserNotifications;
using Application.Notifications.Commands.MarkNotificationAsRead;

namespace API.Controllers
{
	[Authorize]
    [ApiController]
    public class NotificationsController : ApiController
    {
        [HttpGet("all")]
        public Task<IEnumerable<NotificationList>> Get([FromQuery] GetCurrentUserNotificationsQuery query)
        {
            return Mediator.Send(query);
        }

        [HttpPost("markAsRead")]
        public Task<MediatR.Unit> Delete([FromBody] MarkNotificationAsReadCommand command)
        {
            return Mediator.Send(command);
        }
    }
}