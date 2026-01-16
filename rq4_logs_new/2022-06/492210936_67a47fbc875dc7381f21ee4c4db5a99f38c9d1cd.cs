using MongoDB.Driver;
using Simsprojekat.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simsprojekat.Controller
{
    class PriceListController
    {
        private readonly IMongoDatabase _database;
        public IMongoCollection<PriceList> priceListCollection;

        public PriceListController()
        {
            var settings = MongoClientSettings.FromConnectionString("mongodb+srv://PrlinaRozicBibinErdelji:PrlinaRozicBibinErdelji@cluster0.5qfgf.mongodb.net/?retryWrites=true&w=majority");
            var client = new MongoClient(settings);
            _database = client.GetDatabase("TollBooth");
            this.priceListCollection = _database.GetCollection<PriceList>("PriceList");
        }

        public PriceList getActivePriceList()
        {
            var priceLists = _database.GetCollection<PriceList>("PriceList");
            return priceLists.Find(priceList => priceList.IsActive == true).FirstOrDefault();
        }
    }
}