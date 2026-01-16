namespace Carter.App.Route.NewSignUp
{
    using System;

    using Carter;

    using Carter.App.Lib.Authentication;
    using Carter.App.Lib.Generate;

    using Carter.App.Route.PreSecurity;

    using Microsoft.AspNetCore.Http;

    using MongoDB.Bson;
    using MongoDB.Driver;

    public class NewSignUp : CarterModule
    {
        public NewSignUp(IMongoDatabase db)
            : base("/users")
        {
            this.Before += PreSecurity.GetSecurityCheck(db);

            this.Post("/signUp/", async (req, res) =>
            {
                string newToken = Generate.GetRandomToken();
                var signUpTokens = db.GetCollection<BsonDocument>("users.tokens.signUp");

                var tokenDoc = new
                {
                    hash = PasswordHasher.Hash(newToken),
                    expirationSeconds = 86400, // One day
                    createdAt = new BsonDateTime(DateTime.Now),
                };

                await signUpTokens.InsertOneAsync(tokenDoc.ToBsonDocument());

                await res.WriteAsync(newToken);
            });
        }
    }
}