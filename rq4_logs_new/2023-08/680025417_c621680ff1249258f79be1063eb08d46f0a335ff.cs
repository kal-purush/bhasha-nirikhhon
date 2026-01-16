using ShopOnline.Models.Dtos;
using System.Net.Http.Json;

namespace ShopOnline.Web.Services.Contracts
{
    public class ShoppingCartService : IShoppingCartService
    {
        private readonly HttpClient _httpClient;

        public ShoppingCartService(HttpClient httpClient) 
        {
            _httpClient = httpClient;
        }
        public async Task<CartItemDto> AddItem(CartItemToAddDto cartItemToAddDto)
        {
            try
            {
                var respnse = await _httpClient.PostAsJsonAsync<CartItemToAddDto>("api/ShoppingCart", cartItemToAddDto);
                if (respnse.IsSuccessStatusCode)
                {
                    if (respnse.StatusCode == System.Net.HttpStatusCode.NoContent)
                    {
                        return default(CartItemDto);
                    }
                    return await respnse.Content.ReadFromJsonAsync<CartItemDto>();

                }
                else
                {
                    var message = await respnse.Content.ReadAsStringAsync();
                    throw new Exception($"Http status:{respnse.StatusCode} -- {message}");
                }
            }
            catch (Exception)
            {

                throw;
            }
        }

        //public Task<IEnumerable<CartItemDto>> GetItems(int userId)
        //{
        //    try
        //    {
        //        var respnse = await _httpClient.GetAsync($"api/{userId}/GetItems");
        //        if (respnse.IsSuccessStatusCode)
        //        {

        //        }
        //    catch (Exception)
        //    {

        //        throw;
        //    }
        //}
    }
}