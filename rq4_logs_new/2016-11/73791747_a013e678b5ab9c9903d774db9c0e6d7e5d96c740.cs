using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AuctionNet.Interfaces;

namespace AuctionNet.Controllers
{
    public class BidController : IBidController
    {
        private AuctionNetModel _auctionNetModel;

        public BidController()
        {
            _auctionNetModel = new AuctionNetModel();
        }
        public List<Bids> GetAllBids()
        {
            return _auctionNetModel.Bids.ToList();
        }

        public List<object> GetHighestBidPerAuction()
        {
            throw new NotImplementedException();
        }

        public List<ValidationResult> Create(Bids bid)
        {
            ValidationContext context = new ValidationContext(bid, null, null);
            List<ValidationResult> result = new List<ValidationResult>();
            bool valid = Validator.TryValidateObject(bid, context, result, true);
            if (valid)
            {
                _auctionNetModel.Bids.Add(bid);
                _auctionNetModel.SaveChanges();
            }

            return result;
        }
    }
}