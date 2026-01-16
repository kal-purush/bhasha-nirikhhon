using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BikeShoppa.Model
{
    class Bike
    {
        public BikeType type { get; set; }
        public BikeGender gender { get; set; }

        public int size { get; set; }
        public int hourRate { get; set; }
        public int dailyRate { get; set; }


        public enum BikeGender
        {
            Male,
            Female
        }

        public enum BikeType
        {
            EBike,
            PedalBike,
            ChildBike
        }
    }
}