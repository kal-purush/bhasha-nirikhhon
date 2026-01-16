using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;

namespace SocialSerivce.Controllers
{
    
        public class TokenedApiController : ApiController
        {
            protected bool IsAuthorized()
            {
                string[] tokenUser = Request.Headers.Authorization.ToString().Split();
                if (tokenUser == default(string[]) || tokenUser.Length < 2)
                    return false;
                return GetToken(tokenUser[0])==tokenUser[1];
            }
            protected static string GetToken(string user)
            {
                if (user == "user")
                    return "zzzzzzzzzz";
                return "hh";
            }
        }
    
}