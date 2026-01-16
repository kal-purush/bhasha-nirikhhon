using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NeedyBuddy.Models
{
    public class DetailsViewModel
    {

        public DetailsViewModel()
        {
            this.serviceDetailsViewModel = new List<ServiceDetailsViewModel>();
        }
        public string Id { get; set; }

        public string UserName { get; set; }
        public string FirstName { get; set; }

        public string ContactNumber { get; set; }

        public string Email { get; set; }

        public string City { get; set; }

        public string Pincode { get; set; }

        public string ServiceName { get; set; }

        public string Address { get; set; }
        public List<ServiceDetailsViewModel> serviceDetailsViewModel { get; set; }
    }
}