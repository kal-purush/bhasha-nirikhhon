

namespace Project_Server.Entities

{
    public class Conversion
    {       
        public string BaseCode { get; set; }
        public List<ExchangeRates> ConversionRates { get; set; } = new List<ExchangeRates>();
    }
}