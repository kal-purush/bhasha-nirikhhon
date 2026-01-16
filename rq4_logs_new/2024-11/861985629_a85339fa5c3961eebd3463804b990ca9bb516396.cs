using Airbnb.Application.Rea_Time;
using MediatR;
using Microsoft.AspNetCore.SignalR;

namespace Airbnb.Application.Features.Notifications
{
	public class NotificationEventHandler : INotificationHandler<NotificationEvent>
	{
		private readonly IHubContext<NotificationHub> _hubContext;
		private readonly UserConnectionManager _userConnectionManager;

		public NotificationEventHandler(IHubContext<NotificationHub> hubContext, UserConnectionManager userConnectionManager)
		{
			_hubContext = hubContext;
			_userConnectionManager = userConnectionManager;
		}


		public async Task Handle(NotificationEvent notification, CancellationToken cancellationToken)
		{

			if (notification.IsPublic)
			{
				await _hubContext.Clients.All.SendAsync("ReceiveNotification", notification.Message);
			}
			else
			{
				var connectionId = _userConnectionManager.GetConnectionId(notification.UserId);
				if (connectionId != null)
				{
					await _hubContext.Clients.Client(connectionId).SendAsync("ReceiveNotification", notification.Message);
				}
			}
		}
	}
}