using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HelpI.API.Games.Domain.Models;
using HelpI.API.Games.Domain.Services;
using HelpI.API.Games.Domain.Services.Communications;
using IGDB;
using IGDB.Models;

namespace HelpI.API.Games.Application 
{
    public class GamesApiFacade : IGameService
    {
        private const string IgdbClientId = "ov3od6hqyjyb0iurx0hbbvp6ijde7v";
        private const string IgdbClientSecret = "7jtzb99a7qf3yplg6m0jvst5d5wbp1";

        public async Task<GameResponse> GetGameByIdAsync(int gameId)
        {
            IGDBClient client = new IGDBClient(IgdbClientId, IgdbClientSecret);
            var games = await client.QueryAsync<Game>(IGDBClient.Endpoints.Games, query: $"fields *; where id = {gameId};");
            if (games == null) 
                return new GameResponse($"Game not found with gameId: {gameId}");
            var covers = await client.QueryAsync<Cover>(IGDBClient.Endpoints.Covers, query: $"fields *; where game = {gameId};");
            var cover = covers.First();
            var game = games.First();
            var gameModel = new GameModel(game.Id, game.Name, game.Storyline, game.Summary,cover.Url,cover.Height,cover.Width);
            return new GameResponse(gameModel);
        }
    }
}