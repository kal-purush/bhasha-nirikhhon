using AutoMapper;
using Waitlistme.Domain.Models;
using Waitlistme.Resources;

namespace Waitlistme.Profiles {
    public class PartyProfile : Profile {
        public PartyProfile() {
            CreateMap<PartyProfile, PartyReadDto>();
        }
    }

}