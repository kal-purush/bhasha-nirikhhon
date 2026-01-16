using LookieLooks.Api.Interfaces;
using LookieLooks.Api.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LookieLooks.Api.Services
{
    public class GameService : IGameService
    {
        public Task<Guid> CloseGameAsync(Guid gameId)
        {
            //TODO - call function from repo to turn Game.IsBallotOpen = false
            throw new NotImplementedException();
        }

        public Task<Guid> CreateGameAsync(int productId, Guid attributeId)
        {
            Game newGame = new Game()
            {
                AttributeId = attributeId,
                ProductId = productId,
                Created = DateTime.Now,
                GameId = Guid.NewGuid(),
                IsBallotOpen = true
            };

            //TODO - call function from repo to add newly created game "newGame" to repo
            throw new NotImplementedException();
        }

        public Task<IEnumerable<Vote>> GetCurrentVotesAsync(Guid gameId)
        {
            //TODO - call function from repo to get Votes where gameId == gameId
            throw new NotImplementedException();
        }

        public Task<Game> GetRandomGameAsync(Guid userId)
        {
            //TODO - call function from repo to get game with highest number of votes that isn't closed and user hasn't voted in yet
            throw new NotImplementedException();
        }

        public Task<IEnumerable<Game>> GetUserGamesAsync(Guid userId)
        {
            //TODO - call function from repo to get Games with votes where userId = userId
            throw new NotImplementedException();
        }

        public Task<int> GetUserPointsAsync(Guid userId)
        {
            //TODO - call function from repo to get score where userId=userId
            throw new NotImplementedException();
        }
    }
}