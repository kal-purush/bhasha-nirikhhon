using System.Collections.Generic;
using Fsd.Pzpn.Crew.Api.Entities;
using Fsd.Pzpn.Crew.Api.Services;

namespace Fsd.Pzpn.Crew.Services.Players
{
    public class PlayersService : IPlayersService
    {
        public IEnumerable<Player> GetAll()
        {
            return new List<Player>
            {
                new Player
                {
                    Id = 1,
                    FirstName = "Robert",
                    LastName = "Lewandowski",
                    Number = 9
                },
                new Player
                {
                    Id = 1,
                    FirstName = "Kamil",
                    LastName = "Glik",
                    Number = 4
                },
                new Player
                {
                    Id = 1,
                    FirstName = "Kamil",
                    LastName = "Grosicki",
                    Number = 10
                }
            }; 
        }
    }
}