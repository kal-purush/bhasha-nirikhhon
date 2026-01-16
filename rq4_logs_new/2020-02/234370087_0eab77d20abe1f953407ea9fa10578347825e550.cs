using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TinyClothes.Models
{
    public static class SessionHelper
    {
        private const string IDKey = "ID";
        private const string UsernameKey = "Username";

        public static void CreateUserSession(int ID, string username, IHttpContextAccessor http)
        {
            http.HttpContext.Session.SetInt32(IDKey, value : ID);
            http.HttpContext.Session.SetString(UsernameKey, value : username);
        }

        /// <summary>
        /// Logs user out. Clears all sessions.
        /// </summary>
        /// <param name="http"></param>
        public static void DestroyUserSession(IHttpContextAccessor http)
        {
            http.HttpContext.Session.Clear();
        }

        /// <summary>
        /// Returns true if the user is logged in
        /// </summary>
        /// <param name="http"></param>
        public static bool IsUserLoggedIn(IHttpContextAccessor http)
        {
            return http.HttpContext.Session.GetInt32(IDKey).HasValue;
        }
    }
}