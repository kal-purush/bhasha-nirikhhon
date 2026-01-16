using IP5.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace IP5.Repositories
{

    public interface IPlayersRepository
    {
        List<Player> GetPlayers();
    }

    public class PlayersRepository: IPlayersRepository
    {
        public List<Player> GetPlayers()
        {
            List<Player> players = new List<Player>()
            {
                new Player {Code = "jl" , Description = "Joel", Wins = 3, Losses = 6},
                new Player {Code = "jan", Description = "Jan", Wins = 5, Losses = 11, IsChampion = true},
                new Player {Code = "andy" , Description = "Andy", Wins = 11, Losses = 9}
            };

            return players;
        }
    }
}