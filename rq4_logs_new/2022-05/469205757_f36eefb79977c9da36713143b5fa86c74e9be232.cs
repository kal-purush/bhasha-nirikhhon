using AutoMapper;
using CarResale.DAL.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CarResale.BL.Models
{
    public record CarListModel(
            string ModelName,
            string ManufacturerName,
            string CarType,
            string ImagePath
        ) : ModelBase 
    {
        public string ModelName { get; set; } = ModelName;
        public string ManufacturerName { get; set; } = ManufacturerName;
        public string CarType { get; set; } = CarType;
        public string ImagePath { get; set; } = ImagePath;

        public class MapperProfile : Profile 
        { 
            public MapperProfile()
            {
                CreateMap<CarEntity, CarListModel>()
                    .IncludeMembers(src => src.CarModel)
                    .ConstructUsing(src => new CarListModel("", "", "", ""));
                CreateMap<CarModelEntity, CarListModel>()
                    .ConstructUsing(src => new CarListModel("", "", "", ""))
                    .IncludeMembers(src => src.Type, src => src.Manufacturer)
                    .ForMember(dest => dest.CarType, opt => opt.MapFrom(src => src.Type.Type))
                    .ForMember(dest=> dest.ManufacturerName, opt => opt.MapFrom(src => src.Manufacturer.ManufacturerName));

                CreateMap<CarTypeEntity, CarListModel>()
                    .ConstructUsing(src => new CarListModel("", "", "", ""));
;
                CreateMap<CarManufacturerEntity, CarListModel>()
                    .ConstructUsing(src => new CarListModel("", "", "", ""));
;

            }        
        }
    }
}