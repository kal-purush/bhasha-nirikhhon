using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Simsprojekat.Model;


namespace Simsprojekat.Controller
{
    internal class TransactionController
    {
        private readonly IMongoDatabase _database;
        public IMongoCollection<Transaction> transactionCollection;
        public TransactionController()
        {
            var settings = MongoClientSettings.FromConnectionString("mongodb+srv://PrlinaRozicBibinErdelji:PrlinaRozicBibinErdelji@cluster0.5qfgf.mongodb.net/?retryWrites=true&w=majority");
            var client = new MongoClient(settings);
            _database = client.GetDatabase("TollBooth");
            this.transactionCollection = _database.GetCollection<Transaction>("Transactions");
        }

        public List<Transaction> GetAll()
        {
            return transactionCollection.Find(item => true).ToList();
        }

        public Transaction GetById(int id)
        {
            return transactionCollection.Find(item => item.Id == id).FirstOrDefault();
        }

        public void Insert(Transaction t)
        {
            transactionCollection.InsertOne(t);
        }
        public void Delete(int id)
        {
            transactionCollection.DeleteOne(item => item.Id == id);
        }
        public void Update(Transaction t)
        {
            transactionCollection.ReplaceOne(item => item.Id == t.Id, t);
        }
        public List<Transaction> GetAllByTollStation(int id)
        {
            List<Transaction> certainTransactions = new List<Transaction>();
            List < Transaction > transactions = GetAll();
            foreach(Transaction transaction in transactions)
            {
                if(transaction.ExitStationId == id)
                {
                    certainTransactions.Add(transaction);
                }
            }
            return certainTransactions;
        }

    }
}