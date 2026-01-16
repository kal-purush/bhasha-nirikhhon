using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using Simsprojekat.Model;

namespace Simsprojekat.Controller
{
    internal class TicketController
    {
        private readonly IMongoDatabase _database;
        public IMongoCollection<Ticket> ticketCollection;
        public TicketController()
        {
            var settings = MongoClientSettings.FromConnectionString("mongodb+srv://PrlinaRozicBibinErdelji:PrlinaRozicBibinErdelji@cluster0.5qfgf.mongodb.net/?retryWrites=true&w=majority");
            var client = new MongoClient(settings);
            _database = client.GetDatabase("TollBooth");
            this.ticketCollection = _database.GetCollection<Ticket>("Tickets");
        }

        public Ticket GetById(int id)
        {
            return ticketCollection.Find(item => item.Id == id).FirstOrDefault();
        }
        public List<Ticket> GetAll()
        {
            return ticketCollection.Find(item => true).ToList();
        }

        public bool Insert(Ticket ts)
        {
            var tickets = _database.GetCollection<Ticket>("Tickets");

            var id = tickets.Find(e => true).SortByDescending(e => e.Id).FirstOrDefault().Id;
            ts.Id = id + 1;

            tickets.InsertOne(ts);


            return true;
        }

        public bool Update(Ticket t)
        {
            var tickets = _database.GetCollection<Ticket>("Tickets");

            tickets.ReplaceOne(u => u.Id == t.Id, t);


            return true;
        }


        public bool Delete(int ticketId)
        {
            var tickets = _database.GetCollection<TollStation>("Tickets");

            tickets.DeleteOne(ts => ts.Id == ticketId);

            return true;

        }
    }
}