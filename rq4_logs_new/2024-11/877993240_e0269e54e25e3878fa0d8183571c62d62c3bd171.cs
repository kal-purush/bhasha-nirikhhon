using AutoMapper;
using Domain.Entities;
using Domain.Models.Creates;
using Domain.Models.Updates;
using Domain.Models.Views;

namespace Application.Mappings;

public class MappingProfile : Profile
{
    public MappingProfile()
    {
        CreateMap<Role, RoleViewModel>();
        // .ForMember(dest => dest.Ten, opt => opt.MapFrom(src => src.Name))

        // .ForMember(dest => dest.NgayTao, opt => opt.Ignore());

        CreateMap<RoleCreateModel, Role>()
            .ForMember(dest => dest.Id, opt => opt.MapFrom(src => Guid.NewGuid()));


        CreateMap<RoleUpdateModel, Role>()
            .ForAllMembers(opts => opts.Condition((_, _, srcMember) => srcMember != null));

    }
}