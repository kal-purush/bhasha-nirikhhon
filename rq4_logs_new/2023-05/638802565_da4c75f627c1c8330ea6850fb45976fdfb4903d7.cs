using AutoMapper;
using CRM_api.DataAccess.Models;
using CRM_api.DataAccess.ResponseModel;
using CRM_api.Services.Dtos.AddDataDto.HR_Module;
using CRM_api.Services.Dtos.ResponseDto.HR_Module;

namespace CRM_api.Services.MapperProfile
{
    public class DepartmentProfile : Profile
    {
        public DepartmentProfile()
        {
            CreateMap<AddDepartmentDto, TblDepartmentMaster>().ReverseMap();
            CreateMap<DepartmentResponse, DisplayDepartmentDto>();
            CreateMap<TblDepartmentMaster, DepartmentDto>();
            CreateMap<UpdateDepartmentDto, TblDepartmentMaster>();
        }
    }
}