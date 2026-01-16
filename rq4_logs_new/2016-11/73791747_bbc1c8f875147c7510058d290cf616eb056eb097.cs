using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AuctionNet.Interfaces
{
    interface IAuctionController
    {
        void ValidateInput();
        void SaveAuction();

        List<Auctions> GetAllAuctions();

        List<Auctions> GetAuctionsHighestBids();

        List<Auctions> GetAuctionsesFromDate(DateTime start, DateTime end);

        List<object> GetAuctionsViewCommission(DateTime start, DateTime end);


    }
}