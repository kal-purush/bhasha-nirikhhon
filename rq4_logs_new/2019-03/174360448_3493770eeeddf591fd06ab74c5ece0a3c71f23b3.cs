using APILayer.Client.Contracts;
using APILayer.Entities.BadgeService;
using APILayer.Entities.PostsService;
using APILayer.Entities.UsersService;
using System;
using System.Collections.Generic;
using System.Text;

namespace APILayer.Client.Services
{
    public class UsersServiceRestApi : IUsersServiceRestApi
    {
        public List<UsersBadgeCount> GetUserBadgeCount()
        {
            throw new NotImplementedException();
        }

        public RootResponse UsersServiceGetFromGzip(UsersRequest postsRequest)
        {
            throw new NotImplementedException();
        }
    }
}