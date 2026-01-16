using Application.Utils;
using AutoMapper;
using Contract.Services.PaidSalary.ShareDtos;
using Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Mappers;

public class PaidSalaryMappingProfile : Profile
{
    public PaidSalaryMappingProfile()
    {
        CreateMap<PaidSalary, PaidSalaryResponse>()
            .ForCtorParam("CreatedAt", opt => opt.MapFrom(src => DateUtil.ConvertStringToDateTimeOnly(src.CreatedDate.Date.ToString())))
            ;
    }
}