using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TinyClothes.Models
{
    /// <summary>
    /// Helper class to manage users shopping cart data using cookies
    /// </summary>
    public static class CartHelper
    {
        public static void Add(Clothing c, IHttpContextAccessor http)
        {
            string data = JsonConvert.SerializeObject(c);

            CookieOptions options = new CookieOptions()
            {
                Expires = DateTime.Now.AddDays(14),
                IsEssential = true,
                Secure = true
            };

            http.HttpContext.Response.Cookies.Append("CartCookie", data, options);
        }

        public static int GetItemCount(IHttpContextAccessor http)
        {
            throw new NotImplementedException();
        }

        public static List<Clothing> GetAllClothes(IHttpContextAccessor http)
        {
            throw new NotImplementedException();
        }
    }
}