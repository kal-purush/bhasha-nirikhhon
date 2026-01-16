using eCommerce.Models;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace eCommerce
{
    public static class CookieHelper
    {
        public const string cartCookie = "cartCookie";

        public static List<Product> GetProductsFromCart(IHttpContextAccessor accessor)
        {
            string existingItems = accessor.HttpContext.Request.Cookies[cartCookie];
            // add new item to old items
            List<Product> cartList = new List<Product>();
            if (existingItems != null)
            {
                cartList = JsonConvert.DeserializeObject<List<Product>>(existingItems);
            }

            return cartList;
        }

        public static void AddItemToCart(IHttpContextAccessor accessor, Product p)
        {
            List<Product> cartList = GetProductsFromCart(accessor);
            cartList.Add(p);
            
            string data = JsonConvert.SerializeObject(cartList);
            CookieOptions options = new CookieOptions
            {
                Expires = DateTime.Now.AddDays(30),
                Secure = true,
                IsEssential = true,
            };

            accessor.HttpContext.Response.Cookies.Append(cartCookie, data, options);
        }

        public static int GetTotalNumberOfCartProducts(IHttpContextAccessor accessor)
        {
            return GetProductsFromCart(accessor).Count;
        }
    }
}