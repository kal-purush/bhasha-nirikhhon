using BookWebEcommerce.Data.Cart;
using BookWebEcommerce.Data.ViewModel;
using BookWebEcommerce.Models;
using Microsoft.AspNetCore.Mvc;

namespace BookWebEcommerce.Controllers
{
    public class OrderController : Controller
    {
        private readonly ShoppingCart _shoppingCart;

        public OrderController(ShoppingCart shoppingCart)
        {
            _shoppingCart = shoppingCart;
        }

        public IActionResult ShoppingCart()
        {
            var items = _shoppingCart.GetShoppingCartItems();
            _shoppingCart.ShoppingCartItems = items;

            var response = new ShoppingCartVM()
            {
                shoppingCart = _shoppingCart,
                ShoppingCartTotal = _shoppingCart.GetShoppingCartTotal()
            };
            return View(response);
        }
    }
}