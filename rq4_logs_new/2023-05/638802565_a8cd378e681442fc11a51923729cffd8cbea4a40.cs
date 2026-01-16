using AutoMapper;
using CRM_api.DataAccess.Models;
using CRM_api.DataAccess.ResponseModel.Generic_Response;
using CRM_api.Services.Dtos.AddDataDto.Business_Module.LI_GI_Module;
using CRM_api.Services.Dtos.ResponseDto.Business_Module.LI_GI_Module;
using CRM_api.Services.Dtos.ResponseDto.Generic_Response;

namespace CRM_api.Services.MapperProfile
{
    public class InsuranceClientProfile : Profile
    {
        public InsuranceClientProfile()
        {
            CreateMap<AddInsuranceClientDto, TblInsuranceclient>();
            CreateMap<UpdateInsuranceClientDto, TblInsuranceclient>();

            CreateMap<TblInsuranceclient, InsuranceClientDto>();
            CreateMap<TblInsuranceCompanylist, InsuranceCompanyListDto>();
            CreateMap<TblInvesmentType, InvesmentTypeDto>();
            CreateMap<TblSubInvesmentType, SubInvesmentTypeDto>();
            CreateMap<TblInsuranceTypeMaster, InsuranceTypeMasterDto>();

            CreateMap<Response<TblInsuranceclient>, ResponseDto<InsuranceClientDto>>();
            CreateMap<Response<TblInsuranceCompanylist>, ResponseDto<InsuranceCompanyListDto>>();
        }
    }
}