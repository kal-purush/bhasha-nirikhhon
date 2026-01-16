using AutoMapper;
using LinkBakery.Application.Contracts.Persistence;
using LinkBakery.Domain.Entities;
using MediatR;

namespace LinkBakery.Application.Features.TrackingLinks.Commands.CreateTrackingLink
{
    public class CreateTrackingLinkCommandHandler : IRequestHandler<CreateTrackingLinkCommand, int>
    {
        private readonly ITrackingLinkRepository _trackingLinkRepository;
        private readonly IMapper _mapper;


        public CreateTrackingLinkCommandHandler(
            ITrackingLinkRepository trackingLinkRepository,
            IMapper mapper
        )
        {
            _trackingLinkRepository = trackingLinkRepository;
            _mapper = mapper;
        }

        public async Task<int> Handle(CreateTrackingLinkCommand request, CancellationToken cancellationToken)
        {
            var trackingLink = await _trackingLinkRepository.InsertAndSafeAsync(_mapper.Map<TrackingLink>(request));

            if (trackingLink == null)
            {
                return 0;
            }

            return trackingLink.Id;
        }
    }
}