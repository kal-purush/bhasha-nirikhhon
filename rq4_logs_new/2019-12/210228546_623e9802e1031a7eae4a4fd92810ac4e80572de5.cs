namespace Carter.App.Route.PreSecurity
{
    using System;
    using System.Threading.Tasks;

    using Carter.App.Lib.Authentication;

    using Microsoft.AspNetCore.Http;

    using MongoDB.Bson;
    using MongoDB.Driver;

    public static class PreSecurity
    {
        public static Func<HttpContext, Task<bool>> GetSecurityCheck(IMongoDatabase db)
        {
            return async (ctx) =>
            {
                var accessTokens = db.GetCollection<BsonDocument>("users.tokens.access");

                string token = ctx.Request.Headers["ExperienceCapture-Access-Token"];
                if (token == null)
                {
                    ctx.Response.StatusCode = 400;
                    return false;
                }

                var filterClaims = Builders<BsonDocument>.Filter.Eq("hash", PasswordHasher.Hash(token));
                var claimDoc = await accessTokens.Find(filterClaims).FirstOrDefaultAsync();

                if (claimDoc == null)
                {
                    ctx.Response.StatusCode = 401;
                    return false;
                }

                return true;        
            };
        }
    }
}