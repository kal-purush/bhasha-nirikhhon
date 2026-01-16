namespace WeatherAPI.Models
{
    public class ForecastWeatherModel
    {
        public string City { get; set; } // TODO: TEMP
#nullable enable
        public string?[]? DayOfWeek { get; set; }
        public string?[]? SunriseTimeLocal { get; set; }
        public string?[]? SunsetTimeLocal { get; set; }

        public string?[]? Narrative { get; set; }
        public float?[]? Qpf { get; set; } // Quantitative Precipitation Forecast: Closest thing to Precip24Hour in current
        public int?[]? TemperatureMax { get; set; }
        public int?[]? TemperatureMin { get; set; }
        public DayPart?[]? DayPart { get; set; }
    }

    public class DayPart // Night and day parts of forecast
    {
        public string?[]? DayOrNight { get; set; }
        public string?[]? DayPartName { get; set; }
        public int?[]? UvIndex { get; set; }
        public string?[]? VxPhraseLong { get; set; }
    }
}