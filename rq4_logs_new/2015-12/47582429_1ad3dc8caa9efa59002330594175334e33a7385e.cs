using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AntiMoneyHackEmailer
{
    public class PlayerInfo
    {
        public string suspectedPlayerName { get; set; }
        public string playerId { get; set; }
        public string databaseId { get; set; }
        public string moneyInBank { get; set; }
        public string cashOnHand { get; set; }

        }
}