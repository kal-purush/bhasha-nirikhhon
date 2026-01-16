using AutoMapper;
using LinkBakery.Core.Repositories.Interfaces;
using LinkBakery.Web.Cms.Dtos;
using LinkBakery.Web.Cms.Services.Interfaces;

namespace LinkBakery.Web.Cms.Services
{
    public class TrackingLinkService : ITrackingLinkService
    {
        private readonly ITrackingLinkRepository _trackingLinkRepository;
        private readonly IMapper _mapper;


        public TrackingLinkService(
            ITrackingLinkRepository trackingLinkRepository,
            IMapper mapper)
        {
            _trackingLinkRepository = trackingLinkRepository;
            _mapper = mapper;
        }


        public async Task<IEnumerable<TrackingLinkDto>> GetAllAsync()
            => _mapper.Map<IEnumerable<TrackingLinkDto>>(await _trackingLinkRepository.GetAllAsync());

    }
}