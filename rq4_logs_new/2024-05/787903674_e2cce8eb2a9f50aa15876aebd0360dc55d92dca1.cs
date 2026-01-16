using Mambo.Mongo.Models;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TokenService.Domain
{
    public sealed class TokenEntity : BaseMongoEntity<ObjectId>
    {
        public TokenEntity()
        {
            UserId = string.Empty;
            AccessToken = string.Empty;
            RefreshToken = string.Empty;
        }

        private TokenEntity(string userId, string accessToken, string refreshToken)
        {
            UserId = userId;
            AccessToken = accessToken;
            RefreshToken = refreshToken;
        }

        public string UserId { get; private set; }
        public string AccessToken { get; private set; }
        public string RefreshToken { get; private set; }

        public static TokenEntity CreateNewTokenEntity(string userId, string accessToken, string refreshToken)
        {
            return new TokenEntity(userId, accessToken, refreshToken);
        }
    }
}