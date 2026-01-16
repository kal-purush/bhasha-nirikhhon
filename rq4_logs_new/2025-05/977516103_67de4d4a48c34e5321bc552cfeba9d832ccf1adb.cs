using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GoogleMaps_FinalProjectAspApi.DAL.Entities
{
    public class Place
    {
        public Guid Id { get; set; }
        public string PlaceId { get; set; }
        public string Name { get; set; }
        public string NationalPhoneNumber { get; set; }
        public double Longitude { get; set; }
        public double Latitude { get; set; }
        public string FormattedAddress { get; set; }
        public double Rating { get; set; }
        public string GoogleMapsUri { get; set; }
    }
}