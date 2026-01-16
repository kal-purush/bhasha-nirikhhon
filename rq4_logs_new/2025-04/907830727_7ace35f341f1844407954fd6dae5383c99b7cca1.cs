using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Factories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyTravel.Application.Factories
{
    public class AgencyFactory : IAgencyFactory
    {
        public Agency CreateInstance()
        {
            return new Agency
            {
                Id = Guid.NewGuid(),
                Name = string.Empty,
                Address = string.Empty,
                ContactNumber = string.Empty,
                Website = string.Empty,
                LicenseNumber = string.Empty,
                AddDate = DateTime.Now
            };
        }
    }
}