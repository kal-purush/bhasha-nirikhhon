using System;
namespace InterdimensionalThings.Models
{
    public class SettingsModel
    {
        public string Language { get; set; }
        public string Currency { get; set; }

        public SettingsModel()
        {
            Language = "English";
            Currency = "Dollar";
        }
    }
}