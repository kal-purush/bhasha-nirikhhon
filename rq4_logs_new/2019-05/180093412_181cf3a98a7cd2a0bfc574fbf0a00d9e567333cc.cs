using System;
using Akavache;
using System.Reactive.Linq;
using PDGT.Models;
using System.Threading.Tasks;
using System.Collections.Generic;
using PDGT.Services;

namespace PDGT.Stores
{
    public class TokenStore
    {
        public static User GetUserToken()
        {
            User CurrentUser = new User();

            try
            {
                CurrentUser = BlobCache.Secure.GetObject<User>("CurrentUser").Wait();
            }
            catch (KeyNotFoundException)
            {
                AuthenticationService.SetUserToken("1234");
            }
            return CurrentUser;
        }
    }
}