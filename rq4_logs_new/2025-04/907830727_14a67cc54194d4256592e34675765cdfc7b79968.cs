using EasyTravel.Domain.Entites;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace EasyTravel.Web.Views.Shared.Components.RecommendationCarousel
{
    public class RecommendationCarouselViewComponent : ViewComponent
    {
        private readonly HttpClient _httpClient;

        public RecommendationCarouselViewComponent(IHttpClientFactory httpClientFactory)
        {
            _httpClient = httpClientFactory.CreateClient();
        }

        public async Task<IViewComponentResult> InvokeAsync(string type, int count = 5)
        {
            var endpoint = $"https://localhost:44336/api/recommendation/{type}?count={count}";
            var response = await _httpClient.GetStringAsync(endpoint);
            var recommendations = JsonConvert.DeserializeObject<List<RecommendationDto>>(response);

            return View("_CarouselPartial", recommendations);
        }
    }

}