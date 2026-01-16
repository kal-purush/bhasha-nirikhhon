using BlazorEcommerce.Shared;
using System.Net.Http.Json;

namespace BlazorEcommerce.Client.Services.ProductService
{
    public class ProductService : IProductService
    {
        private readonly HttpClient _http;
        public ProductService(HttpClient http)
        {
            _http = http;
        }


        public async Task<List<GetProductDTO>> GetAllProducts()
        {
            return await _http.GetFromJsonAsync<List<GetProductDTO>>("api/product/get-products");
        }

      
    }
}