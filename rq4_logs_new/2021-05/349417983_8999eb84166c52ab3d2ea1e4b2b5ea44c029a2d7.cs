using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WebStore.Domain.Entities;
using WebStore.Domain.ViewModels;
using WebStore.Interfaces.Services;

namespace WebStore.Services.Products
{
    public class CartService : ICartService
    {
        private readonly IProductData _prductData;
        private ICartStore _cartStore;

        public CartService(IProductData prductData, ICartStore cartStore)
        {
            _prductData = prductData;
            _cartStore = cartStore;
        }

        public void ReplaceCookie(IResponseCookies responseCookies, string key, string value)
        {
            responseCookies.Delete(key);
            responseCookies.Append(key, value);
        }

        public void AddToCart(int id)
        {
            Cart cart = _cartStore.Cart;

            CartItem cartItem = cart.Items.FirstOrDefault(i => i.ProductId == id);

            if (cartItem != null)
            {
                cartItem.Quantity++;
            }
            else
            {
                cart.Items.Add(new CartItem { ProductId = id, Quantity = 1 });
            }

            _cartStore.Cart = cart;
        }

        public void DecrementFromCart(int id)
        {
            Cart cart = _cartStore.Cart;

            CartItem cartItem = cart.Items.FirstOrDefault(i => i.ProductId == id);

            if (cartItem is null)
                return;

            if (cartItem.Quantity > 0)
                cartItem.Quantity--;

            if (cartItem.Quantity == 0)
                cart.Items.Remove(cartItem);

            _cartStore.Cart = cart;
        }

        public void RemoveFromCart(int id)
        {
            Cart cart = _cartStore.Cart;

            CartItem cartItem = cart.Items.FirstOrDefault(i => i.ProductId == id);

            if (cartItem != null)
            {
                cart.Items.Remove(cartItem);
            }

            _cartStore.Cart = cart;
        }

        public void Clear()
        {
            Cart cart = _cartStore.Cart;

            cart.Items.Clear();

            _cartStore.Cart = cart;
        }

        public CartViewModel TransformToViewModel()
        {
            Cart cart = _cartStore.Cart;

            var products = _prductData.GetProducts(new Domain.ProductFilter() { ProductsIds = cart.Items.Select(i => i.ProductId).ToArray() });

            return new CartViewModel
            {
                Items = cart.Items.Select(i => (new ProductViewModel
                {
                    Id = i.ProductId,
                    Name = products.FirstOrDefault(p => p.Id == i.ProductId)?.Name,
                    Brand = products.FirstOrDefault(p => p.Id == i.ProductId)?.Brand.Name,
                    Price = products.FirstOrDefault(p => p.Id == i.ProductId).Price,
                    ImageUrl = products.FirstOrDefault(p => p.Id == i.ProductId)?.ImageUrl,
                }, i.Quantity))
            };
        }
    }
}