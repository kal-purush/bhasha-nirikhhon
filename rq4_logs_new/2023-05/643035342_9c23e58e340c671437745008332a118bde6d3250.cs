using System;

namespace WeatherAPI.DTO
{
    /// <summary>
    /// Base for WeatherDTO and used against HistoricalWeatherModel since it has very few props
    /// </summary>
    public class BaseWeatherDTO
    {
        public string City { get; set; }
        public string Date { get; set; }
        public string DayOfWeek { get; set; }
        public float? Precipitation { get; set; }
        public int? TemperatureMax { get; set; }
        public int? TemperatureMin { get; set; }
        public string Description { get; set; }
    }
}