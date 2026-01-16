using AutoMapper;
using Mybnb.api.Models;
using Mybnb.dtolibrary.DTOs;
using Mybnb.dtolibrary.DTOs.BNB;
using Mybnb.dtolibrary.DTOs.User;

namespace Mybnb.api.Mapper
{
    public class MappingProfile : Profile
    {
        public MappingProfile()
        {
            // Response DTO'S
            CreateMap<BNB, BNBResponse>();
            CreateMap<PossibleRentingPeriod, PossibleRentingPeriodResponse>();
            CreateMap<TenantPeriod, TenantPeriodResponse>();
            CreateMap<User, UserResponse>();
            CreateMap<User, AuthenticateResponse>();

            // Request DTO'S
            CreateMap<BNBRequest, BNB>();
            CreateMap<PossibleRentingPeriodRequest, PossibleRentingPeriod>();
            CreateMap<CreateUser, User>();
            CreateMap<AuthenticateRequest, AuthenticateResponse>();
        }
    }
}