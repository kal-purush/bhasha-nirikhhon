using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using eCommerce.Data;
using eCommerce.Models;
using Microsoft.AspNetCore.Mvc;

namespace eCommerce.Controllers
{
    public class CartController : Controller
    {
        private readonly ProductContext _context;

        public CartController(ProductContext context)
        {
            _context = context;
        }

        /// <summary>
        /// dds a product to the shopping cart
        /// </summary>
        /// <param name="id">the id of the product to add</param>
        /// <returns></returns>
        public async Task<IActionResult> Add(int id)
        {
            // grab item from db
            Product p = await ProductDB.GetProductByIdAsync(_context, id);
            // add item to cookies


            // redirect them back to index
            return View();
        }

        public IActionResult Summary()
        {
            //display all items in shopping cart

            return View();
        }
    }
}