using AutoMapper;
using Services.AuthAPI.Models;
using Services.AuthAPI.Models.Dto;

namespace Services.AuthAPI
{
    public class MappingConfig
    {
        public static MapperConfiguration RegisterMaps()
        {
            var mappingConfig = new MapperConfiguration(config =>
            {
                config.CreateMap<ApplicationUser, UserDto>()
                    .ForMember(dest => dest.ID, opt => opt.MapFrom(src => src.Id));
                    
                config.CreateMap<RegistrationRequestDto, ApplicationUser>()
                    .ForMember(dest => dest.UserName, opt => opt.MapFrom(src => src.Email))
                    .ForMember(dest => dest.NormalizedEmail, opt => opt.MapFrom(src => src.Email.ToUpper()));
            });
            return mappingConfig;
        }
    }
}