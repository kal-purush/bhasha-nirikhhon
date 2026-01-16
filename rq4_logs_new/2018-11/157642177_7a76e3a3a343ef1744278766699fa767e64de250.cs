using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CIS560.Address
{
    public class Address
    {
        private int addressId { get; }
        private string line1 { get; }
        private string line2 { get; }
        private string city { get; }
        private string state { get; }
        private int zip { get; }

        public Address(int addressId, string line1, string line2, string city, string state, int zip)
        {
            this.addressId = addressId;
            this.line1 = line1;
            this.line2 = line2;
            this.city = city;
            this.state = state;
            this.zip = zip;
        }
    }
}