using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models.Entities
{
    public class FuturesAccountBalance
    {
        public string AccountAlias { get; set; }              // Unique account code
        public string Asset { get; set; }                     // Asset name
        public decimal Balance { get; set; }                  // Wallet balance
        public decimal CrossWalletBalance { get; set; }       // Crossed wallet balance
        public decimal CrossUnPnl { get; set; }               // Unrealized profit of crossed positions
        public decimal AvailableBalance { get; set; }         // Available balance
        public decimal MaxWithdrawAmount { get; set; }        // Maximum withdrawal amount
        public bool MarginAvailable { get; set; }             // Whether the asset can be used as margin
        public long UpdateTime { get; set; }                  // Update timestamp
    }
}