using System;
namespace ACM.BL
{
    public class Address
    {
        public Address()
        {
        }
        public Address(int addressId)
        {
            AddressId = addressId;
        }

        public int AddressId { get; private set; }
        public int AddressType { get; set; }
        public string City { get; set; }
        public string Country { get; set; }
        public string PostalCode { get; set; }
        public string State { get; set; }
        public string AddressLine1 { get; set; }
        public string AddressLine2 { get; set; }

        public bool Validate()
        {
            if (PostalCode == null)
            {
                return false;
            }
            else if (AddressLine1 == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    }
}