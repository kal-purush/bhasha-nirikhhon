using GoogleMaps_FinalProjectAspApi.Abstract;
using System.Text.Json;

namespace GoogleMaps_FinalProjectAspApi.Core
{
    public class GeocodingService : IGeocodingService
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey;
        private readonly string _geocodeBaseUrl;

        public GeocodingService(HttpClient httpClient, IConfiguration configuration)
        {
            _httpClient = httpClient;
            _apiKey = configuration["RequestData:ApiKey"];
            _geocodeBaseUrl = configuration["RequestData:RequestUriGeocode"];
        }

        public async Task<(double Latitude, double Longitude, float RadiusMeters)?> GetCoordinatesAsync(string address)
        {
            var encodedAddress = Uri.EscapeDataString(address);
            var url = $"{_geocodeBaseUrl}?address={encodedAddress}&key={_apiKey}";

            var response = await _httpClient.GetAsync(url);
            if (!response.IsSuccessStatusCode)
            {
                return null;
            }

            var content = await response.Content.ReadAsStringAsync();
            var json = JsonDocument.Parse(content);

            if (!json.RootElement.TryGetProperty("results", out var results) || results.GetArrayLength() == 0)
            {
                return null;
            }

            var result = results[0];

            var location = result.GetProperty("geometry").GetProperty("location");
            double centerLat = location.GetProperty("lat").GetDouble();
            double centerLng = location.GetProperty("lng").GetDouble();

            var viewport = result.GetProperty("geometry").GetProperty("viewport");
            var northeast = viewport.GetProperty("northeast");
            var southwest = viewport.GetProperty("southwest");

            double neLat = northeast.GetProperty("lat").GetDouble();
            double neLng = northeast.GetProperty("lng").GetDouble();
            double swLat = southwest.GetProperty("lat").GetDouble();
            double swLng = southwest.GetProperty("lng").GetDouble();

            double distanceToNE = HaversineDistance(centerLat, centerLng, neLat, neLng);
            double distanceToSW = HaversineDistance(centerLat, centerLng, swLat, swLng);

            float averageRadius = (float)((distanceToNE + distanceToSW) / 2.0);

            return (centerLat, centerLng, averageRadius);
        }

        private double HaversineDistance(double lat1, double lon1, double lat2, double lon2)
        {
            const double R = 6371e3;
            var φ1 = lat1 * Math.PI / 180.0;
            var φ2 = lat2 * Math.PI / 180.0;
            var Δφ = (lat2 - lat1) * Math.PI / 180.0;
            var Δλ = (lon2 - lon1) * Math.PI / 180.0;

            var a = Math.Sin(Δφ / 2) * Math.Sin(Δφ / 2) +
                    Math.Cos(φ1) * Math.Cos(φ2) *
                    Math.Sin(Δλ / 2) * Math.Sin(Δλ / 2);

            var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));

            return R * c;
        }

    }
}