using System;
using System.Collections.Generic;

namespace ACM.BL.Repositories
{
    public class AddressRepository
    {
        // Retrieve one address

        public Address Retrieve(int addressId)
        {
            Address address = new Address(addressId);

            // hard coded address because this project has no data access layer
            if (addressId == 1)
            {
                address.AddressType = 1;
                address.AddressLine1 = "123 Main St.";
                address.AddressLine2 = "apt 3";
                address.City = "Nashville";
                address.State = "TN";
                address.Country = "United States";
                address.PostalCode = "37064";
            }
            return address;
        }

        public IEnumerable<Address> RetrieveByCustomerId(int customerId)
        {
            var addressList = new List<Address>();
            Address address = new Address(1)
            {
                AddressType = 1,
                AddressLine1 = "123 Main St.",
                AddressLine2 = "apt 3",
                City = "Nashville",
                State = "TN",
                Country = "United States",
                PostalCode = "37064"
            };
            addressList.Add(address);

            address = new Address(2)
            {
                AddressType = 2,
                AddressLine1 = "Green Dragon",
                City = "Bywater",
                State = "Shire",
                Country = "Middle Earth",
                PostalCode = "146"
            };
            addressList.Add(address);

            return addressList;
        }

        public bool Save(Address address)
        {
            if (address != null)
            {
                return true;
            } else
            {
                return false;
            }
        }
    }
}