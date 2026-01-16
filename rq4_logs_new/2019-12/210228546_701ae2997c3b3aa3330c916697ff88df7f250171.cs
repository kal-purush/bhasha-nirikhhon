namespace Carter.App.Route.Tags
{
    using Carter;

    using Carter.App.Route.PreSecurity;

    using Carter.Request;

    using Microsoft.AspNetCore.Http;

    using MongoDB.Bson;
    using MongoDB.Driver;

    public class Tags : CarterModule
    {
        public Tags(IMongoDatabase db)
            : base("/sessions/{id}/tags")
        {
            this.Before += PreSecurity.GetSecurityCheck(db);

            this.Post("/{tagName}", async (req, res) =>
            {
                var sessions = db.GetCollection<BsonDocument>("sessions");

                string uniqueID = req.RouteValues.As<string>("id");
                var filter = Builders<BsonDocument>.Filter.Eq("id", uniqueID);
                var sessionDoc = await sessions.Find(filter).FirstOrDefaultAsync();

                if (sessionDoc == null)
                {
                    res.StatusCode = 404;
                    return;
                }

                string collectionName = $"sessions.{uniqueID}";
                var sessionCollection = db.GetCollection<BsonDocument>(collectionName);

                BsonArray tags = sessionDoc["tags"].AsBsonArray;

                string tag = req.RouteValues.As<string>("tagName");

                if (!tags.Contains(tag))
                {
                    var update = Builders<BsonDocument>.Update.Set("tags", tags.Add(tag));
                    await sessions.UpdateOneAsync(filter, update);
                }

                res.ContentType = "application/text; charset=utf-8";
                await res.WriteAsync("OK");
            });

            this.Delete("/{tagName}", async (req, res) =>
            {
                var sessions = db.GetCollection<BsonDocument>("sessions");

                string uniqueID = req.RouteValues.As<string>("id");
                var filter = Builders<BsonDocument>.Filter.Eq("id", uniqueID);
                var sessionDoc = await sessions.Find(filter).FirstOrDefaultAsync();

                if (sessionDoc == null)
                {
                    res.StatusCode = 404;
                    return;
                }

                string collectionName = $"sessions.{uniqueID}";
                var sessionCollection = db.GetCollection<BsonDocument>(collectionName);

                BsonArray tags = sessionDoc["tags"].AsBsonArray;

                string tag = req.RouteValues.As<string>("tagName");

                if (!tags.Contains(tag))
                {
                    res.StatusCode = 409;
                    return;
                }

                tags.Remove(tag);
                var update = Builders<BsonDocument>.Update.Set("tags", tags);
                await sessions.UpdateOneAsync(filter, update);

                res.ContentType = "application/text; charset=utf-8";
                await res.WriteAsync("OK");
            });
        }
    }
}