using DisneyCafe.Models.Database;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DisneyCafe.Models
{
    public static class CartHelper
    {
        private const string CartCookie = "Cart";

        public static List<Desserts> GetDesserts(IHttpContextAccessor accessor)
        {
            string data = accessor.HttpContext.Request.Cookies[CartCookie];

            if (string.IsNullOrWhiteSpace(data))
            {
                return new List<Desserts>();
            }

            List<Desserts> desserts =
                JsonConvert.DeserializeObject<List<Desserts>>(data);
            return desserts;
        }

        public static int GetDessertCount(IHttpContextAccessor accessor)
        {
            List<Desserts> allDesserts = GetDesserts(accessor);
            return allDesserts.Count;
        }

        public static void Add(IHttpContextAccessor accessor, Desserts dessert)
        {
            List<Desserts> desserts = GetDesserts(accessor);
            desserts.Add(dessert);
            string data = JsonConvert.SerializeObject(dessert);

            accessor.HttpContext.Response.Cookies.Append(CartCookie, data);
        }
    }
}