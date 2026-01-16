using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CEPiK.Models
{
    public class Car
    {
        public int CarID { get; set; }
        public String CarBrand { get; set; }
        public String Category { get; set; }
        public String Type { get; set; }
        public String Model { get; set; }
        public String Variant { get; set; }
        public String Version { get; set; }

        public ICollection<Vehicle> Vehicles { get; set; }

    }
}