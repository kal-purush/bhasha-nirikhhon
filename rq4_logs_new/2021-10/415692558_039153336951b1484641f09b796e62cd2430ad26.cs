using Auction.LMS.CORE.Data;
using Auction.LMS.CORE.DTO;
using System;
using System.Collections.Generic;
using System.Text;

namespace Auction.LMS.CORE.Services
{
   public interface IAuctionService
    {
        public List<Auction1> GetAllAuction();
        public bool InsertAuction(Auction1 auction);
        public bool UpdateAuction(Auction1 auction);
        public bool DeleteAuction(int id);
        public bool UpdateBidPriceAuction(Auction1 auction);
        public bool UpdateAntiquesStatus(Auction1 auction);
        public List<Auction1> RemainingTime(Auction1 auction);
        public List<AuctionInSpecificIntervalDTO> AuctionInSpecificInterval(AuctionInSpecificIntervalDTO auction);
    }
}