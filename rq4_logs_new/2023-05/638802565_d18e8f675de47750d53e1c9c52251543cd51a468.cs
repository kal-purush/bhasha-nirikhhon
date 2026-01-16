using AutoMapper;
using CRM_api.DataAccess.Models;
using CRM_api.DataAccess.ResponseModel;
using CRM_api.Services.Dtos.AddDataDto.HR_Module;
using CRM_api.Services.Dtos.ResponseDto.HR_Module;

namespace CRM_api.Services.MapperProfile
{
    public class DesignationProfile : Profile
    {
        public DesignationProfile()
        {
            CreateMap<AddDesignationDto, TblDesignationMaster>();
            CreateMap<DesignationResponse, DisplayDesignationDto>();
            CreateMap<TblDesignationMaster, DesignationDto>().ReverseMap();
            CreateMap<UpdateDesignationDto, TblDesignationMaster>();
        }
    }
}