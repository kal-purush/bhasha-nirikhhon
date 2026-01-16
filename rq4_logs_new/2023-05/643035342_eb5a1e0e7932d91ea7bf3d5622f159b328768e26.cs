namespace WeatherAPI.DTO
{
    public class WeatherDTO
    {
        public string City { get; set; }
        public string DayOfWeek { get; set; }
        public float Precipitation { get; set; }
        public string SunriseTime { get; set; }
        public string SunsetTime { get; set; }
        public int Temperature { get; set; }
        public int TemperatureMax { get; set; }
        public int TemperatureMin { get; set; }
        public int UvIndex { get; set; }
        public string WindDirection { get; set; }
        public int WindSpeed { get; set; }
        public string Description { get; set; }
    }
}