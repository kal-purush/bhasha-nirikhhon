using TG.Backend.Exceptions;
using TG.Backend.Models.Offer;
using TG.Backend.Repositories.Offer;

namespace TG.Backend.Features.Offer.EditOffer;

public class EditOfferCommandHandler : IRequestHandler<EditOfferCommand, OfferResponse>
{
    private readonly IOfferRepository _offerRepository;

    public EditOfferCommandHandler(IOfferRepository offerRepository)
    {
        _offerRepository = offerRepository;
    }

    public async Task<OfferResponse> Handle(EditOfferCommand request, CancellationToken cancellationToken)
    {
        var offer = await _offerRepository.GetOfferByIdAsync(request.Id, true);
        if (offer is null)
            throw new OfferNotFoundException(request.Id);

        await _offerRepository.EditOfferAsync(offer, request.EditOfferDto);

        return new OfferResponse
        {
            IsSuccess = true,
            StatusCode = HttpStatusCode.NoContent
        };
    }
}