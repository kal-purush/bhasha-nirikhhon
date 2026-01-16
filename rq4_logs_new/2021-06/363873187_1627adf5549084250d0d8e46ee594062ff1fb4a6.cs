using AutoMapper;
using CSB.Business.Enums;
using CSB.Business.Models;
using CSB.Repository.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSB.Business
{
    public class BusinessProfile : Profile
    {
        public BusinessProfile()
        {
            CreateMap<short, EmployeeStatus>();
            CreateMap<EmployeeStatus, short>();
            CreateMap<short, Gender>();
            CreateMap<Gender, short>();
            CreateMap<CreateEmployeeDto, Employee>()
                .ForMember(dest => dest.Status, opt => opt.MapFrom(_ => (short)EmployeeStatus.Active))
                .ForMember(dest => dest.Gender, opt => opt.MapFrom(src => (short)src.Gender));
            CreateMap<Employee, GetEmployeeDto>();
        }
    }
}