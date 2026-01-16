using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PicnicOrm.Dapper.Demo.Models
{
    public class User
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public DateTime BirthDate { get; set; }

        public int AddressId { get; set; }

        public Address Address { get; set; }

        public int EmployerId { get; set; }

        public Employer Employer { get; set; }

        public List<Car> Cars { get; set; } 
    }
}