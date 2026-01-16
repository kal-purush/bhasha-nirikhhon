using CSharpFunctionalExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace FindAnimal.Domain.Entities.ValueObjects
{
    public record class Address
    {
        public string Street { get; }
        public string HouseNumber { get; }
        public string City { get; }
        public string State { get; }
        public string ZipCode { get; }

        private Address(string street, string houseNumber, string city, string state, string zipCode)
        {
            Street = street;
            HouseNumber = houseNumber;
            City = city;
            State = state;
            ZipCode = zipCode;
        }

        public static Result<Address> Create(string street, string houseNumber, string city, string state, string zipCode)
        {
           return new Address(street, houseNumber, city, state, zipCode);
        }
    }
}