using AutoMapper;
using Contract.Services.Report.ShareDtos;
using Domain.Entities;

namespace Application.Mappers;

public class ReportMappingProfile : Profile
{
    public ReportMappingProfile()
    {
        CreateMap<Report, ReportResponse>()
            .ForCtorParam("StatusName", opt => opt.MapFrom(src => src.Status.ToString()))
            .ForCtorParam("ReportTypeName", opt => opt.MapFrom(src => src.ReportType.ToString()))
            .ForCtorParam("ReportTypeDescription", opt => opt.MapFrom(src => src.ReportType.GetDescription()))
            .ForCtorParam("StatusDesscription", opt => opt.MapFrom(src => src.Status.GetDescription()))
            .ForCtorParam("FullName", opt => opt.MapFrom(src => src.User.FirstName + " " + src.User.LastName))
            .ForCtorParam("UserId", opt => opt.MapFrom(src => src.User.Id))
            .ForCtorParam("CompanyId", opt => opt.MapFrom(src => src.User.CompanyId))
            .ForCtorParam("CreatedDate", opt => opt.MapFrom(src => DateOnly.FromDateTime(src.CreatedDate.Date)));

    }
}