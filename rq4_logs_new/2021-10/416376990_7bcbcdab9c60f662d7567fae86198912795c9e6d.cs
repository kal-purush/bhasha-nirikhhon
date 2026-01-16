using Microsoft.AspNetCore.Mvc;
using PopLibrary.Data;
using PopLibrary.SqlModels;
using System.Linq;

namespace PopApis.Controllers
{
    public class AuctionsController : Controller
    {
        private AuctionData _data;

        public AuctionsController(AuctionData data)
        {
            _data = data;
        }

        // GET: Auction
        public ActionResult Index()
        {
            return View("Index", _data.GetAuctions());
        }

        // GET: Auctions/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Auctions/Create
        [HttpPost]
        public ActionResult Create(Auction itemToAdd)
        {
            _data.AddOrUpdateAuction(itemToAdd);
            return RedirectToAction("Index", _data.GetAuctions());
        }

        public ActionResult Edit(int id)
        {
            return View(_data.GetAuctions().ToList().Find(t => t.Id == id));
        }

        // GET: Auctions/Edit
        [HttpPost]
        public ActionResult Edit(int id, Auction itemToUpdate)
        {
            _data.AddOrUpdateAuction(itemToUpdate);
            return RedirectToAction("Index", _data.GetAuctions());
        }

        public ActionResult Delete(int id)
        {
            _data.DeleteAuction(id);
            return RedirectToAction("Index", _data.GetAuctions());
        }
    }
}