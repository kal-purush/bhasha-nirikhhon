using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using WebAdvertisement.Web.ViewModels.Home;
using WebAdvertisements.Web.DTO;
using WebAdvertisements.Web.ViewModels.Account;
using WebAdvertisements.Web.ViewModels.Home;

namespace WebAdvertisements.Web.Profiles
{
    public class MainProfile : Profile
    {
        public MainProfile()
        {
            CreateMap<Advertisement, IndexViewModel>()
                .ForMember(dest => dest.Image, src => src.MapFrom(field => field.FilePath));

            CreateMap<CreateAdvertisement, CreateAdvertisementViewModel>()
                .ReverseMap();
        }
    }
}