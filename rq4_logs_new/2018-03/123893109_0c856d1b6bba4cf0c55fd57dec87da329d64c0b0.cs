using System.Collections.Generic;

namespace DABHandin2._1
{
    class Person
    {
        public string FirstName { get; set; }
        public string MiddleName { get; set; }
        public string LastName { get; set; }
        public string Context { get; set; }
        public string Email { get; set; }

        public List<TelephoneNumber> TelephoneNumbers { get; set; }

        public Address PrimaryAddress { get; set; }
        public List<Address> AlternativeAddresses { get; set; }
        public List<AddressType> AddressTypes { get; set; }
    }
}