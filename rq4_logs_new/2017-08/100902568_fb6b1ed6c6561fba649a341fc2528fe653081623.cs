using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RouteConfigurator
{
    public class Hub
    {
        public string CallSign { get; set; }
        public int MaxCategory { get; set; }
        public double FlightTax { get; set; }

        public Hub(string callsign, int maxCategory, double flightTax)
        {
            CallSign = callsign;
            MaxCategory = maxCategory;
            FlightTax = flightTax;
        }
    }

}