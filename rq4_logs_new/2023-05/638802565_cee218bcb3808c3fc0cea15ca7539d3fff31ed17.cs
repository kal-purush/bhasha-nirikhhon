using AutoMapper;
using CRM_api.DataAccess.Models;
using CRM_api.Services.Dtos.AddDataDto.Business_Module.MutualFunds_Module;
using CRM_api.Services.Dtos.ResponseDto.Business_Module.MutualFunds_Module;

namespace CRM_api.Services.MapperProfile
{
    public class MutualfundProfile : Profile
    {
        public MutualfundProfile()
        {
            CreateMap<AddMutualfundsDto, TblMftransaction>()
                .AfterMap((dto, Mutualfund) =>
                {
                    Mutualfund.Notes = null;
                });

            CreateMap<AddMutualfundsDto, TblNotexistuserMftransaction>();

            
        }
    }
}