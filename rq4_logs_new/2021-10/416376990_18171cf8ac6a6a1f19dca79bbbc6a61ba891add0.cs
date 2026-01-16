using Microsoft.AspNetCore.Mvc;
using PopLibrary.Data;
using PopLibrary.SqlModels;
using System.Linq;

namespace PopApis.Controllers
{
    public class BidController : CommonController
    {
        private BidData _data;

        public BidController(BidData data)
        {
            _data = data;
        }

        // GET: Bid
        public ActionResult Index()
        {
            return ViewWithSession("Index", _data.GetBids());
        }

        // GET: Bid/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Bid/Create
        [HttpPost]
        public ActionResult Create(GetAuctionBidResult itemToAdd)
        {
            _data.AddOrUpdateBid(itemToAdd);
            return RedirectToAction("Index", _data.GetBids());
        }

        public ActionResult Edit(int id)
        {
            return View(_data.GetBids().ToList().Find(t => t.Id == id));
        }

        // GET: Bid/Edit
        [HttpPost]
        public ActionResult Edit(int id, GetAuctionBidResult itemToUpdate)
        {
            _data.AddOrUpdateBid(itemToUpdate);
            return RedirectToAction("Index", _data.GetBids());
        }

        public ActionResult Delete(int id)
        {
            _data.DeleteAuctionBid(id);
            return RedirectToAction("Index", _data.GetBids());
        }
    }
}