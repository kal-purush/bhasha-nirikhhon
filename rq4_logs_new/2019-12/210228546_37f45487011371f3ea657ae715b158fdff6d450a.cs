namespace Nancy.App.Hosting.Kestrel
{
    using System;

    using MongoDB.Bson;
    using MongoDB.Bson.IO;
    using MongoDB.Bson.Serialization;
    using MongoDB.Driver;

    using Nancy.App.Random;

    using Network;

    public class Users : NancyModule
    {

        public Users(IMongoDatabase db)
            : base("/users/")
        {
            this.Post("/", async (args) =>
            {
                var sessions = db.GetCollection<BsonDocument>("users");

                // Check if sign-up token is valid
                // If not, return 401

                // Else return OK
                
                return "OK";
            });

            this.Post("/{id}", async (args) =>
            {
                var sessions = db.GetCollection<BsonDocument>("users");

                // Check if id token is valid from Google
                // If not, return 401

                // Else return new API token

                return "TOKEN";
            });

        }
    }
}