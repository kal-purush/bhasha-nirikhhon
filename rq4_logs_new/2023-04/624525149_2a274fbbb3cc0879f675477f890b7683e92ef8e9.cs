using TG.Backend.Exceptions;
using TG.Backend.Models.Offer;
using TG.Backend.Repositories.Offer;

namespace TG.Backend.Features.Offer.DeleteOffer;

public class DeleteOfferCommandHandler : IRequestHandler<DeleteOfferCommand, OfferResponse>
{
    private readonly IOfferRepository _offerRepository;

    public DeleteOfferCommandHandler(IOfferRepository offerRepository)
    {
        _offerRepository = offerRepository;
    }

    public async Task<OfferResponse> Handle(DeleteOfferCommand request, CancellationToken cancellationToken)
    {
        var offer = await _offerRepository.GetOfferByIdAsync(request.Id);
        if (offer is null) throw new OfferNotFoundException(request.Id);

        await _offerRepository.DeleteOfferAsync(offer);

        return new OfferResponse
        {
            IsSuccess = true,
            StatusCode = HttpStatusCode.NoContent
        };
    }
}