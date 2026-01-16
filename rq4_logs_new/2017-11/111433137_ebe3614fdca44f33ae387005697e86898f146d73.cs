using RestSharp;
using Retrofit.Net.Attributes.Methods;
using Retrofit.Net.Attributes.Parameters;

namespace exercise3.Models
{
    public interface IWeatherService
    {
        [Get("weather?q={cityName}&APPID=18130b308759be027c247e699d7a0c66")]
        RestResponse<WeatherObject> GetWeatherInfoByCityName([Path("cityName")]string cityName);

        [Get("weather?q={cityName},{countryCode}&APPID=18130b308759be027c247e699d7a0c66")]
        RestResponse<WeatherObject> GetWeatherInfoByCityName([Path("cityName")]string cityName, [Path("countryCode")]string countryCode);

        [Get("weather?id={cityId}&APPID=18130b308759be027c247e699d7a0c66")]
        RestResponse<WeatherObject> GetWeatherInfoByCityId([Path("cityId")]string cityId);

        [Get("weather?lat={lat}&lon={lon}&APPID=18130b308759be027c247e699d7a0c66")]
        RestResponse<WeatherObject> GetWeatherInfoByGeographicCoordinates([Path("lat")]int lat, [Path("lon")]int lon);

        [Get("weather?zip={zipCode},{countryCode}&APPID=18130b308759be027c247e699d7a0c66")]
        RestResponse<WeatherObject> GetWeatherInfoByZipCode([Path("zipCode")]int zipCode, [Path("countryCode")]string countryCode);

    }
}