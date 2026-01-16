using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Threading.Tasks;
using WebApp.Application.Services.Dtos;
using WebApp.Application.Services.Interfaces;
using WebApp.Domain.Models.ViewModels;
using WebApp.Extensions;

namespace WebApp.Application.Services
{
    public class BasketService : IBasketService
    {
        private readonly HttpClient httpClient;
        private readonly IIdentityService _identityService;
        private readonly ILogger<BasketService> _logger;

        public BasketService(HttpClient httpClient, IIdentityService identityService, ILogger<BasketService> logger) 
        {
            this.httpClient = httpClient;
            _identityService = identityService;
            _logger = logger;
        }

        public async Task AddItemToBasket(int productId)
        {
            var model = new
            {
                CatalogItemId = productId,
                Quantity = 1,
                BasketId = _identityService.GetUserName()
            };

            await httpClient.PostAsync("basket/items", model);
        }

        public Task Checkout(BasketDTO basket)
        {
            return httpClient.PostAsync("basket/checkout", basket);
        }

        public async Task<Basket> GetBasket()
        {
            var response = await httpClient.GetResponseAsync<Basket>("basket/" + _identityService.GetUserName());

            return response ?? new Basket() { BuyerId = _identityService.GetUserName() };
        }

        public async Task<Basket> UpdateBasket(Basket basket)
        {
            var response = await httpClient.PostGetResponseAsync<Basket, Basket>("basket/update", basket);

            return response;
        }
    }
}