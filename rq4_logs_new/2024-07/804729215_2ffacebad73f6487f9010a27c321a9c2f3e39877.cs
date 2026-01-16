using AutoMapper;
using Contract.Services.MonthlyCompanySalary.ShareDtos;
using Domain.Entities;

namespace Application.Mappers;

public class MonthlyCompanySalaryMappingProfile : Profile
{
    public MonthlyCompanySalaryMappingProfile()
    {
        CreateMap<MonthlyCompanySalary, MonthlyCompanySalaryResponse>()
            .ForCtorParam("CompanyName", opt => opt.MapFrom(src => src.Company.Name.ToString()))
            .ForCtorParam("DirectorName", opt => opt.MapFrom(src => src.Company.DirectorName.ToString()));
    }
}