using Messages_microservice.model;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Threading;

namespace Messages_microservice.Repository
{
    public class MessageRepository : IMessageRepository
    {
        private readonly IMongoCollection<Message> _messages;

        public MessageRepository()
        {
            var mongoClient = new MongoClient("mongodb://localhost:27017");
            var mongoDatabase = mongoClient.GetDatabase("messages");
            _messages = mongoDatabase.GetCollection<Message>("messages");
        }

        public IEnumerable<Message> GetAll()
        { 
            return _messages.Find(_ => true).ToList();
        }

        public Message Add(Message message)
        { 
            _messages.InsertOne(message);
            return message;
        }
    }
}