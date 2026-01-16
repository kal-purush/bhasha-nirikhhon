using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ECommerceSite.Data;
using ECommerceSite.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace ECommerceSite.Controllers
{
    public class CartController : Controller
    {
        private readonly ProductContext _context;
        private readonly IHttpContextAccessor _httpcontext;

        public CartController(ProductContext context, IHttpContextAccessor httpContext)
        {
            _context = context;
            _httpcontext = httpContext;
        }

        /// <summary>
        /// Adds a product to the shopping cart
        /// </summary>
        /// <param name="id">The id of the product to add</param>
        /// <returns></returns>
        public async Task<IActionResult> Add(int id) // Id of the product to add
        {
            Product product = await ProductDb.GetProductAsync(_context, id);

            CookieHelper.AddProductToCart(_httpcontext, product);

            // Redirect to previous page
            return RedirectToAction("Index", "Product");
        }

        public IActionResult Summary()
        {
            List<Product> cartProducts = CookieHelper.GetCartProducts(_httpcontext);
            return View(cartProducts); 
        }    
    }
}
