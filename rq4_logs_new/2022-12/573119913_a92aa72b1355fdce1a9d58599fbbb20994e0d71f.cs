using AutoMapper;
using VanKassa.Domain.Dtos;
using VanKassa.Domain.ViewModels;

namespace VanKassa.Backend.Core.AutoMappersConfig;

public class DbEntitiesToViewModelsMapper : Profile
{

    public DbEntitiesToViewModelsMapper()
    {
        CreateMap<EmployeesDbDto, EmployeeViewModel>()
            .ForMember(mem => mem.Id,
                opt => opt.MapFrom(src => src.UserId))
            .ForMember(mem => mem.Addresses,
                opt => opt.MapFrom(src => src.Addresses))
            .ForMember(mem => mem.LastName,
                opt => opt.MapFrom(src => src.LastName))
            .ForMember(mem => mem.FirstName,
                opt => opt.MapFrom(src => src.FirstName))
            .ForMember(opt => opt.Patronymic,
                opt => opt.MapFrom(src => src.Patronymic))
            .ForMember(opt => opt.Role,
                opt => opt.MapFrom(src => src.RoleName))
            .ForMember(mem => mem.Photo,
                opt => opt.MapFrom(src => src.Photo));
    }
}