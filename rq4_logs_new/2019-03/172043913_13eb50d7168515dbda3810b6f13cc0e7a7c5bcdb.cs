using System;
using System.Collections.Generic;
using System.Linq;
using Integratieproject1.DAL.Repositories;
using Integratieproject1.Domain.Datatypes;

namespace Integratieproject1.BL.Managers
{
    public class DataTypeManager
    {
        private DataTypeRepostiory dataTypeRepository;
        private UnitOfWorkManager unitOfWorkManager;

        public DataTypeManager()
        {
            unitOfWorkManager = new UnitOfWorkManager();
            dataTypeRepository = new DataTypeRepostiory(unitOfWorkManager.UnitOfWork);
        }
        public DataTypeManager( UnitOfWorkManager unitOfWorkManager)
        {
            if (unitOfWorkManager == null)
                throw new ArgumentNullException("unitOfWorkManager");

            this.unitOfWorkManager = unitOfWorkManager;
            dataTypeRepository = new DataTypeRepostiory(this.unitOfWorkManager.UnitOfWork);
        }

        public Location CheckLocation(Location toCheckLocation)
        {
            IList<Location> locations = dataTypeRepository.GetLocations().ToList();
            Location returnLocation = toCheckLocation;
            Address address = CheckAddress(toCheckLocation.Address);
            if (address != null)
            {
                returnLocation.Address = address ;
                                
            }
            foreach (Location location in locations)
            {
                if (toCheckLocation.LocationName.Equals(location.LocationName) 
                    && address == location.Address)
                {
                    return location;
                }
                
            }
            return returnLocation;
        }

        public Address GetAddress(int addressId)
        {
            return dataTypeRepository.GetAddress(addressId);
        }

        public Address CheckAddress(Address toCheckAddress)
        {
            IList<Address> addresses = dataTypeRepository.GetAddresses().ToList();
            foreach (Address address in addresses)
            {
                if (toCheckAddress.City.Equals(address.City) 
                    && toCheckAddress.Street.Equals(address.Street) 
                    && toCheckAddress.ZipCode.Equals(address.ZipCode)
                    && toCheckAddress.HouseNr == address.HouseNr)
                {
                    return address;
                }
            }
            return null;
        }
    }
}