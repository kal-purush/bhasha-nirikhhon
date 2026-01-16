using BlazorEcommerce.Shared;
using System.Net.Http.Json;

namespace BlazorEcommerce.Client.Services.LocationService
{
    public class LocationService : ILocationService
    {
        private readonly HttpClient _http;
        public LocationService(HttpClient http)
        {
            _http = http;
        }
        public async Task<List<GetLocationDTO>> GetLocations()
        {
            return await _http.GetFromJsonAsync<List<GetLocationDTO>>("api/location/get-locations");
        }
    }
}