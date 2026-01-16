using MongoDB.Driver;
using Simsprojekat.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simsprojekat.Controller
{
    internal class PriceListController
    {

        private readonly IMongoDatabase _database;

        public PriceListController()
        {
            var settings = MongoClientSettings.FromConnectionString("mongodb+srv://PrlinaRozicBibinErdelji:PrlinaRozicBibinErdelji@cluster0.5qfgf.mongodb.net/?retryWrites=true&w=majority");
            var client = new MongoClient(settings);
            _database = client.GetDatabase("TollBooth");
        }

        public List<PriceList> GetAll()
        {
            var priceLists = _database.GetCollection<PriceList>("PriceList");
            return priceLists.Find(item => true).ToList();
        }

        public PriceList GetActive()
        {
            var priceLists = _database.GetCollection<PriceList>("PriceList");
            return priceLists.Find(item => item.IsActive == true).FirstOrDefault();
        }

        public PriceList GetById(int id)
        {
            var priceLists = _database.GetCollection<PriceList>("PriceList");
            return priceLists.Find(item => item.Id == id).FirstOrDefault();
        }

        public bool Insert(PriceList priceList)
        {
            var priceLists = _database.GetCollection<PriceList>("PriceList");

            var id = priceLists.Find(e => true).SortByDescending(e => e.Id).FirstOrDefault().Id;
            priceList.Id = id + 1;

            priceLists.InsertOne(priceList);

            return true;
        }

        public bool Update(PriceList priceList)
        {
            var priceLists = _database.GetCollection<PriceList>("PriceList");

            priceLists.ReplaceOne(u => u.Id == priceList.Id, priceList);

            return true;
        }


        public bool Delete(int priceListId)
        {
            var priceLists = _database.GetCollection<PriceList>("PriceList");

            priceLists.DeleteOne(ts => ts.Id == priceListId);

            return true;

        }





    }
}