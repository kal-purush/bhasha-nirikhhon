using MongoDB.Driver;

namespace AvaloniaGUI
{
    public class MongoDBContext
    {
        public IMongoDatabase Database { get; }

        public MongoDBContext(string connectionString, string databaseName)
        {
            var client = new MongoClient(connectionString);
            Database = client.GetDatabase(databaseName);
        }

        // Add additional methods or properties as needed for accessing MongoDB collections
    }
}