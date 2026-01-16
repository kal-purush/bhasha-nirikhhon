using Microsoft.AspNetCore.Mvc;
using PopLibrary.Data;
using PopLibrary.SqlModels;
using System.Linq;

namespace PopApis.Controllers
{
    public class BidsController : Controller
    {
        private AuctionBidData _data;

        public BidsController(AuctionBidData data)
        {
            _data = data;
        }

        // GET: AuctionBid
        public ActionResult Index()
        {
            return View("Index", _data.GetAuctionBids());
        }

        // GET: AuctionBid/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: AuctionBid/Create
        [HttpPost]
        public ActionResult Create(AuctionBid itemToAdd)
        {
            _data.AddOrUpdateAuctionBid(itemToAdd);
            return RedirectToAction("Index", _data.GetAuctionBids());
        }

        public ActionResult Edit(int id)
        {
            return View(_data.GetAuctionBids().ToList().Find(t => t.Id == id));
        }

        // POST: AuctionBid/Edit
        [HttpPost]
        public ActionResult Edit(int id, AuctionBid itemToUpdate)
        {
            _data.AddOrUpdateAuctionBid(itemToUpdate);
            return RedirectToAction("Index", _data.GetAuctionBids());
        }

        public ActionResult Delete(int id)
        {
            _data.DeleteAuctionBid(id);
            return RedirectToAction("Index", _data.GetAuctionBids());
        }
    }
}