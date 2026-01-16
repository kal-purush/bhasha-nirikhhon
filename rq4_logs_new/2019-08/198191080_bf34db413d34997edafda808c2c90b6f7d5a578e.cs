using AutoMapper;
using NProducts.Data.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NProducts.Web.Models
{
    public static class CategoriesMapping
    {
        public static Categories ConvertToCategories(this CategoriesDTO item)
        {
            // Настройка AutoMapper
            var config = new MapperConfiguration(cfg =>
            {
                cfg.CreateMap<CategoriesDTO, Categories>();
            });

            var mapper = config.CreateMapper();
            return mapper.Map<CategoriesDTO, Categories>(item);
        }

        public static CategoriesDTO ConvertToCategoriesDTO(this Categories item)
        {
            // Настройка AutoMapper
            var config = new MapperConfiguration(cfg =>
            {
                cfg.CreateMap<Categories, CategoriesDTO>();
            });

            var mapper = config.CreateMapper();
            return mapper.Map<Categories, CategoriesDTO>(item);
        }

        public static IEnumerable<CategoriesDTO> ConvertToCategoriesDTO(this IEnumerable<Categories> items)
        {
            // Настройка AutoMapper
            var config = new MapperConfiguration(cfg =>
            {
                cfg.CreateMap<Categories, CategoriesDTO>();
            });

            var mapper = config.CreateMapper();
            return mapper.Map<IEnumerable<Categories>, IEnumerable<CategoriesDTO>>(items);
        }
    }
}