using CSB.Business.Interfaces;
using CSB.Repository.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSB.Business.Impl
{
    class AddressService : IAddressService
    {
        private readonly IAddressService _addressRepository;

        public AddressService(IAddressService addressRepository)
        {
            this._addressRepository = addressRepository;
        }
        public async Task<Address> GetAddressesByIdAsync(int employeeId)
        {
            if (employeeId <= 0)
            {
                throw new ArgumentException();
            }
            return await _addressRepository.GetAddressesByIdAsync(employeeId);
        }
    }
}