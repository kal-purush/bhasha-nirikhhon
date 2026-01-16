using AutoMapper;
using Identity.Application.DTO;
using Identity.Domain.AggregationModels.ApplicationUser.Child;

namespace Identity.Api.Configuration;

public class AutoMapperProfile: Profile
{
    public AutoMapperProfile()
    {
        CreateMap<CountryInfo, CountryInfoDto>();
    }
}