using System;
using System.Collections.Generic;
using Rentalbase.Models;

namespace Rentalbase.ViewModels
{
    public class LandlordIndexData
    {
        public IEnumerable<Landlord> Landlords { get; set; }
        public IEnumerable<Tenant> Tenants { get; set; }
        public IEnumerable<Lease> Leases { get; set; }
        public IEnumerable<Property> Properties { get; set; }
        public int FreeLandlords { get; set; }
    }
}