using Application.DTOs.Brand;
using AutoMapper;

namespace Application.DTOs.BrandConfig.Mapping
{
    public class BrandProfile : Profile
    {
        public BrandProfile()
        {
            CreateMap<Domain.Entities.Brand, BrandDto>()
                .ForMember(dest => dest.Id, opt => opt.MapFrom(src => src.Id))
                .ForMember(dest => dest.Name, opt => opt.MapFrom(src => src.BrandName))
                .ForMember(dest => dest.ImageUrl, opt => opt.MapFrom(src => src.BrandImage));

            CreateMap<BrandDto, Domain.Entities.Brand>();
        }
    }
}