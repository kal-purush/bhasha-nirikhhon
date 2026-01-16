using AutoMapper;
using RMS.Application.Requests.CustomerRequests;
using RMS.Application.Responses.CustomerResponses;
using RMS.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RMS.Application.Mappers
{
    internal class AutoMapperConfiguration : Profile
    {
        public AutoMapperConfiguration() 
        {
            CreateMap<CustomerRequestModel, Customer>();
            CreateMap<CustomerResponseModel, Customer>();
            CreateMap<UpdateCustomerRequestModel, Customer>();
        }
    }
}