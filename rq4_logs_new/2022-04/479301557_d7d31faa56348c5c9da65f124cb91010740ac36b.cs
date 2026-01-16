using AutoMapper;
using LinkBakery.Application.Contracts.Persistence;
using MediatR;

namespace LinkBakery.Application.Features.TrackingLinks.Queries.GetTrackingLinkRedirectUrl
{
    public class GetTrackingLinkRedirectUrlQueryHandler : IRequestHandler<GetTrackingLinkRedirectUrlQuery, TrackingLinkRedirectUrlVm>
    {
        private readonly ITrackingLinkRepository _trackingLinkRepository;
        private readonly IMapper _mapper;


        public GetTrackingLinkRedirectUrlQueryHandler(
                ITrackingLinkRepository trackingLinkRepository,
                IMapper mapper
        )
        {
            _trackingLinkRepository = trackingLinkRepository;
            _mapper = mapper;
        }



        public async Task<TrackingLinkRedirectUrlVm> Handle(GetTrackingLinkRedirectUrlQuery request, CancellationToken cancellationToken)
        {
            return _mapper.Map<TrackingLinkRedirectUrlVm>(_trackingLinkRepository.FindActiveByKey(request.Key));
        }
    }
}