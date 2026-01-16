using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PopApis.Models
{
    public class AuctionViewModel
    {
        // The type of auction (ex. live or silent)
        public int AuctionType { get; set; }

        // When the auction completed.
        public DateTime AuctionTime { get; set; }

        // The name of the auction item.
        public string AuctionName { get; set; }

        // Bid information corresponding to the highest bid.
        public BidViewModel HighestBid { get; set; }
    }
}