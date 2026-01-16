using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace Client
{
    class ClientPlayroom : IPlayroom<Player>
    {
        public string CompetitionText => throw new NotImplementedException();

        public List<Player> Players { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public DateTime GameStartingTime { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public DateTime GameEndingTime { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public DateTime TimeToWaitForOpponents { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public Player GetPlayer(string name)
        {
            throw new NotImplementedException();
        }
    }
}