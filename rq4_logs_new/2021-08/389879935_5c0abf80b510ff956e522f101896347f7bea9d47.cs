using System.Collections.Generic;
using System.Linq;
using CarsInfo.BLL.Models.Dtos;
using CarsInfo.DAL.Entities;

namespace CarsInfo.BLL.Mappers
{
    public class BrandServiceMapper
    {
        public BrandDto MapToBrandDto(Brand brand)
        {
            if (brand is null)
            {
                return null;
            }

            return new BrandDto
            {
                Id = brand.Id,
                Name = brand.Name
            };
        }

        public ICollection<BrandDto> MapToBrandsDtos(IEnumerable<Brand> brands)
        {
            return brands?.Select(MapToBrandDto).ToList();
        }

        public Brand MapToBrand(BrandDto brand)
        {
            if (brand is null)
            {
                return null;
            }

            return new Brand
            {
                Id = brand.Id,
                Name = brand.Name
            };
        }
    }
}