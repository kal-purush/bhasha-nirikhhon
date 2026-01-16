using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace fix_it_tracker_back_end.Model.BindingTargets
{
    public class CustomerData
    {
        public string Name { get; set; }

        public string Address { get; set; }

        public string PostalCode { get; set; }

        public string City { get; set; }

        public string Province { get; set; }

        public Customer Customer => new Customer
        {
            Name = Name,
            Address = Address,
            City = City,
            Province = Province,
            PostalCode = PostalCode
        };
    }
}