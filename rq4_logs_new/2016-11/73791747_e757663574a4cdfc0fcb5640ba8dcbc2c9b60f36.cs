using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AuctionNet.Forms;
using AuctionNet.Interfaces;

namespace AuctionNet.Controllers
{
   public class AuctionController:IAuctionController
   {

       private readonly AuctionNetModel _auctionNetModel;

       public AuctionController()
       {
           _auctionNetModel = new AuctionNetModel();
       }
        public void SaveAuction()
        {
            Auctions auction = new Auctions
            {
                AcceptPrice = Convert.ToDecimal(FrmAddAuction.FormAddAuc.txtAcceptPrice.Text),
                StartTime = Convert.ToDateTime(FrmAddAuction.FormAddAuc.txtStartTime.Text),
                EndTime = Convert.ToDateTime(FrmAddAuction.FormAddAuc.txtEndtime.Text),
                ProductId = Convert.ToInt16(FrmAddAuction.FormAddAuc.txtProductId.Text)
            };
            _auctionNetModel.Auctions.Add(auction);
            _auctionNetModel.SaveChanges();
        }

        public List<Auctions> GetAllAuctions()
        {
            return _auctionNetModel.Auctions.ToList();
        }

        public List<object> GetAuctionsHighestBids()
        {
            var auctionHighestBid = GetAllAuctions();
            var list = auctionHighestBid.Where(a=>a.Bids = 

           
            return new List<object>(auctionHighestBid);
        }

        public List<Auctions> GetAuctionsesFromDate(DateTime start, DateTime end)
        {
            throw new NotImplementedException();
        }

        public List<object> GetAuctionsViewCommission(DateTime start, DateTime end)
        {
            throw new NotImplementedException();
        }
    }
}