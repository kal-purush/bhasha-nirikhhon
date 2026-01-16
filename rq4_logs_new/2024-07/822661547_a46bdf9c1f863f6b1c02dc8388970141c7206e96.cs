using AutoMapper;
using ShortSharing.BLL.Models;
using ShortSharing.DAL.Entities;

namespace ShortSharing.BLL.Mappers
{
    public sealed class MapperBllProfile : Profile
    {
        public MapperBllProfile()
        {
            CreateMap<UserEntity, UserModel>().ReverseMap();

            CreateMap<TypeEntity, TypeModel>().ReverseMap();

            CreateMap<CategoryEntity, CategoryModel>().ReverseMap();

            CreateMap<RentEntity, RentModel>().ReverseMap();

            CreateMap<ThingEntity, ThingModel>().ReverseMap();
        }
    }
}