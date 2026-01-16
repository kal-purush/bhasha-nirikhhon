using BusinessLayer.Interfaces;
using DatabaseLayer;
using Microsoft.AspNetCore.Mvc;
using System;

namespace BookStoreApp.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CartController : ControllerBase
    {
        private readonly ICartBL cartBL;
        public CartController(ICartBL cartBL)
        {
            this.cartBL = cartBL;
        }

        [HttpPost("addBooksInCart")]
        public IActionResult AddBookToCart(AddToCart cartBook)
        {
            try
            {
                var result = this.cartBL.AddBookToCart(cartBook);
                if (result.Equals("book added in cart successfully"))
                {
                    return this.Ok(new { success = true, message = $"Book added in Cart Successfully " });
                }
                else
                {
                    return this.BadRequest(new { Status = false, Message = result });
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

    }
}