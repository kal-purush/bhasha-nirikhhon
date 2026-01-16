using AutoMapper;
using CRM_api.DataAccess.Models;
using CRM_api.DataAccess.ResponseModel.Generic_Response;
using CRM_api.Services.Dtos.AddDataDto.HR_Module;
using CRM_api.Services.Dtos.ResponseDto.Generic_Response;
using CRM_api.Services.Dtos.ResponseDto.HR_Module;

namespace CRM_api.Services.MapperProfile
{
    public class LeaveTypeProfile : Profile
    {
        public LeaveTypeProfile()
        {
            CreateMap<AddLeaveTypeDto, TblLeaveType>().ReverseMap();
            CreateMap<TblLeaveType, LeaveTypeDto>();
            CreateMap<UpdateLeaveTypeDto, TblLeaveType>();

            CreateMap<TblLeaveType, LeaveTypeDto>().ReverseMap();
            CreateMap<Response<TblLeaveType>, ResponseDto<LeaveTypeDto>>();
        }
    }
}