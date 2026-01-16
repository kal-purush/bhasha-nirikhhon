using AutoMapper;
using LinkBakery.Application.Contracts.Persistence;
using LinkBakery.Domain.Entities;
using MediatR;

namespace LinkBakery.Application.Features.TrackingLinks.Commands.UpdateTrackingLink
{
    public class UpdateTrackingLinkCommandHandler : IRequestHandler<UpdateTrackingLinkCommand, Unit>
    {
        private readonly ITrackingLinkRepository _trackingLinkRepository;
        private readonly IMapper _mapper;


        public UpdateTrackingLinkCommandHandler(
            ITrackingLinkRepository trackingLinkRepository,
            IMapper mapper
        )
        {
            _trackingLinkRepository = trackingLinkRepository;
            _mapper = mapper;
        }

        public async Task<Unit> Handle(UpdateTrackingLinkCommand request, CancellationToken cancellationToken)
        {
            var trackingLink = await _trackingLinkRepository.GetByIdAsync(request.Id);

            if (trackingLink == null)
            {
                throw new Exception();
            }

            // TODO: Add Validation

            _mapper.Map(request, trackingLink, typeof(UpdateTrackingLinkCommand), typeof(TrackingLink));

            await _trackingLinkRepository.UpdateAsync(trackingLink);

            return Unit.Value;
        }
    }
}