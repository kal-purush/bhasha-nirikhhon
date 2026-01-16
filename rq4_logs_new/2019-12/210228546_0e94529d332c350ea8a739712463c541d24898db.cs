namespace Carter.App.Lib.Mongo
{

    using System.Threading.Tasks;

    using MongoDB.Bson;
    using MongoDB.Driver;

    public static class MongoExtra
    {
        public static async Task<BsonDocument> FindEqAsync(this IMongoCollection<BsonDocument> col, string property, string value)
        {
            var filter = Builders<BsonDocument>.Filter.Eq(property, value);
            return await col.Find(filter).FirstOrDefaultAsync();
        }

        public static async Task<BsonDocument> FindEqAsync(this IMongoCollection<BsonDocument> col, string property, int value)
        {
            var filter = Builders<BsonDocument>.Filter.Eq(property, value);
            return await col.Find(filter).FirstOrDefaultAsync();
        }
    }
}