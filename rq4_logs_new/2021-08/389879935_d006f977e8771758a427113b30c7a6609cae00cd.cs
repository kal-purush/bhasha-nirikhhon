using System.Collections.Generic;
using System.Linq;
using CarsInfo.BLL.Contracts;
using CarsInfo.BLL.Models.Dtos;
using CarsInfo.DAL.Assistance;

namespace CarsInfo.BLL.Services
{
    public class FilterService : IFilterService
    {
        public IList<FilterModel> ConfigureCarFilter(FilterDto filter)
        {
            var filters = new List<FilterModel>();

            if (filter.Brands.Any())
            {
                var brands = string.Join(", ", filter.Brands.Select(b => $"'{b.ToLower()}'"));
                filters.Add(new FilterModel("LOWER(Brand.Name)", $"({brands})", "IN", "AND"));
            }

            if (!string.IsNullOrWhiteSpace(filter.Model))
            {
                filters.Add(new FilterModel("LOWER(Car.Model)", $"%{filter.Model.ToLower()}%", "LIKE"));
            }

            return filters;
        }
    }
}