using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.Authorization;

using DataAccess.Interfaces;

using Domains.DataTransferObjects;

namespace API.Hubs
{
	public interface IClientPhotosHub
	{
		Task UpdateThumbnails(IEnumerable<PhotoThumbnailDTO> thumbnails);
	}

	[Authorize]
	public class PhotosHub : Hub<IClientPhotosHub>
	{
		private readonly IAuthService _authService;
		private readonly IConnectionManager _connectionManager;

		public PhotosHub(IAuthService authService, IConnectionManager connectionManager)
		{
			_authService = authService;
			_connectionManager = connectionManager;
		}

		public override Task OnConnectedAsync()
		{
			string userId =_authService.GetCurrentUserId();
			string connectionId = Context.ConnectionId;
			_connectionManager.AddConnection(userId, connectionId);

			return base.OnConnectedAsync();
		}

		public override Task OnDisconnectedAsync(Exception exception)
		{
			_connectionManager.RemoveConnection(Context.ConnectionId);

			return base.OnDisconnectedAsync(exception);
		}
	}
}