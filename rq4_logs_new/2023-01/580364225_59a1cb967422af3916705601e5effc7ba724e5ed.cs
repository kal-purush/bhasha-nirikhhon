using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArraysNew
{
    internal class Address
    {
        public string City { get; }
        public string Street { get; }
        public int NumberOfHouse { get; }
        public int NumberOfFlat { get; }

        public Address(string city, string street, int numberOfHouse, int numberOfFlat)
        {
            this.City = city;
            this.Street = street;
            this.NumberOfHouse = numberOfHouse;
            this.NumberOfFlat = numberOfFlat;
        }
    }
}