using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Platform.Lobby.Api.Clients;
using Platform.Lobby.Api.Hubs;
using Platform.Lobby.Api.Models;

namespace Platform.Lobby.Api.Controllers
{
	[Authorize]
	[ApiController]
	[Route("api/lobby")]
	[Produces("application/json"), Consumes("application/json")]
	public class LobbyController : ControllerBase
	{
		private readonly IHubContext<LobbyHub, ILobbyClient> _hubContext;

		public LobbyController(
			IHubContext<LobbyHub, ILobbyClient> hubContext)
		{
			_hubContext = hubContext ?? throw new ArgumentNullException(nameof(hubContext));
		}

		[HttpGet("lobby")]
		public LobbyModel GetLobby()
		{
			return new LobbyModel
			{
				Code = "",
				Players = new List<PlayerModel> { new PlayerModel { Id= User.Identity?.Name, Role=Enums.Role.Owner } },
				Visibility=Enums.Visibility.Public,
				State=Enums.Status.InLobby
			};
		}

		[HttpPost("chat")]
		public async Task<bool> Play(string message)
		{
			var chat = new ChatModel
			{
				AuthorID = User?.Identity?.Name,
				Message = message,
				TimeStamp = DateTimeOffset.UtcNow
			};

			await _hubContext.Clients.All.ChatSent(chat);

			return true;
		}
	}
}