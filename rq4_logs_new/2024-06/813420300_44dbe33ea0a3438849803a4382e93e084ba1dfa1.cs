using AutoMapper;
using Stock.Management.Api.Database.Entities;
using Stock.Management.Api.DTOs;

namespace Stock.Management.Api.Mappings;


public class CustomerInvestmentProfile : Profile
{
    public CustomerInvestmentProfile()
    {
        CreateMap<CustomerInvestmentDto, Customerinvestment>()
            .ForMember(dst => dst.Customerid, map => map.MapFrom(src => src.Customerid))
            .ForMember(dst => dst.Productid, map => map.MapFrom(src => src.Productid))
            .ForMember(dst => dst.Amount, map => map.MapFrom(src => src.Amount));
    }
}