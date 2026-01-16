using Mango.ServicesOrderAPI.Services.IServices;
using Mango.Services.OrderAPI.Models.DTOs;
using Newtonsoft.Json;

namespace Mango.Services.OrderAPI.Services
{
    public class CartService : ICartService
    {
        private readonly IHttpClientFactory _httpClientFactory;

        public CartService(IHttpClientFactory clientFactory)
        {
            _httpClientFactory = clientFactory;
        }
        public async Task<bool> RemoveCartForUser(string userId)
        {
            bool isCartRemoved = false;
            var client = _httpClientFactory.CreateClient("Cart");
            var response = await client.DeleteAsync($"/api/cart/RemoveCartByUser/{userId}");
            var apiContet = await response.Content.ReadAsStringAsync();
            var resp = JsonConvert.DeserializeObject<ResponseType>(apiContet);
            if (resp.IsSuccess)
            {
                isCartRemoved = true;
            }
            return isCartRemoved;
        }
    }
}