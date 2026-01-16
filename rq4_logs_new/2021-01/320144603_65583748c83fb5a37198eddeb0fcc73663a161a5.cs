using System;
using System.Threading.Tasks;
using Games.Eucre.Api.Clients;
using Games.Eucre.Api.Hubs;
using Games.Eucre.Api.Models;
using Microsoft.AspNetCore.SignalR;

namespace Games.Eucre.Api.Services
{
	public class GameUpdater
	{
		private readonly IHubContext<GameplayHub, IGameplayClient> _hubContext;

		private GameModel? _game = null;

		public GameUpdater(IHubContext<GameplayHub, IGameplayClient> hubContext)
		{
			_hubContext = hubContext ?? throw new ArgumentNullException(nameof(hubContext));
		}

		public Task<GameModel?> GetGameModel()
		{
			return Task.FromResult(_game);
		}

		public async Task SaveGame(GameModel game)
		{
			_game = game;
			await _hubContext.Clients.All.UpdateGame(_game);
		}
	}
}