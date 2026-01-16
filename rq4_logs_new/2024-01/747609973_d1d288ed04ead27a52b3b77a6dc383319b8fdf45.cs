using Taxes.Contracts.Cities.RequestModels;
using Taxes.Contracts.Cities.ResponseModels;
using Taxes.Database.Models;

namespace Taxes.Business.Mappers
{
    internal static class CityMapper
    {
        public static CityResponse Map(City city)
        {
            return new CityResponse
            {
                Id = city.Id,
                Name = city.Name
            };
        }

        public static City Map(CreateCityRequest city)
        {
            return new City
            {
                Name = city.Name
            };
        }
    }
}