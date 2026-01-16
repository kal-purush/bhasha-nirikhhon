using MongoDB.Driver;
using Simsprojekat.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simsprojekat.Controller
{
    internal class TollBoothController
    {
        private readonly IMongoDatabase _database;
        public IMongoCollection<TollBooth> tollBoothCollection;
        public TollBoothController()
        {
            var settings = MongoClientSettings.FromConnectionString("mongodb+srv://PrlinaRozicBibinErdelji:PrlinaRozicBibinErdelji@cluster0.5qfgf.mongodb.net/?retryWrites=true&w=majority");
            var client = new MongoClient(settings);
            _database = client.GetDatabase("TollBooth");
            this.tollBoothCollection = _database.GetCollection<TollBooth>("TollBooths");
        }

        public TollBooth GetById(int id)
        {
            return tollBoothCollection.Find(item => item.Id == id).FirstOrDefault();
        }


    }
}