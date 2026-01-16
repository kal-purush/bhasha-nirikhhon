using ErgastApi.Responses.Models;

namespace BlazorApp.Client.Models
{
    public class BettingDriverModel
    {
        public int Position { get; set; }
        public Driver? DriverInfo { get; set; }
    }
}