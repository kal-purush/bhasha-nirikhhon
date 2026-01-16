using Amazon.DynamoDBv2.Model;
using CommunityNetwork.Common;
using CommunityNetwork.Common.Inerfaces;
using CommunityNetwork.Common.Models;
using CommunityNetWork.Dal;
using CommunityNetWork.Dal.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Authentication.BL
{
    public class ProfilesManager : IProfileService
    {
        private readonly IDBConnector _iDbConnector;
        private readonly IDynamoDBFactory dBFactory;

        public ProfilesManager(IDynamoDBFactory  dBFactory)
        {
            this.dBFactory = dBFactory;
            this._iDbConnector = dBFactory.Create(false);
        }

        public Profile Login(string email, string password)
        {
            var profileConnected = _iDbConnector.Get<Profile>(email, true);
            if (profileConnected?.Password == password)
            {
                GenerateToken(profileConnected.Email);
            }
            else
            {
                return null;
            }
            return profileConnected;
        }

        public bool CheckValidationToken(string email)
        {
            var token = _iDbConnector.Get<TokenModel>(email, true);
            return CheckValidToken(token.TokenCreateTime);
        }

        private void GenerateToken(string email)
        {
            TokenModel tokenModel = new TokenModel()
            {
                Token = Guid.NewGuid(),
                TokenCreateTime = DateTime.Now,
                Email = email
            };
            _iDbConnector.Add(tokenModel);
        }

        private bool CheckValidToken(DateTime tokenCreateTime)
        {
            return DateTime.Now.Minute - tokenCreateTime.Minute < 2;
        }

        public Profile Register(Profile profile)
        {
            _iDbConnector.Add(profile);
            return profile;
        }

        //public Profile GetProfileDetails(string username)
        //{
        //    return dynamoDB.Get<Profile>(username, true);
        //}

        //public void Delete(string username)
        //{
        //    DeleteItemRequest request = new DeleteItemRequest
        //    {
        //        TableName = "Profiles",
        //        Key = new Dictionary<string, AttributeValue>()
        //        {
        //            ["Email"] = new AttributeValue(username)
        //        }
        //    };
        //    dynamoDB.Delete(request);
        //}
    }
}