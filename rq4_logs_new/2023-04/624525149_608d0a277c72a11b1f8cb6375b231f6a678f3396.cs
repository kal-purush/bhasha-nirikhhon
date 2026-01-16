using FluentValidation;
using TG.Backend.Features.Offer.GetOffers;
using TG.Backend.Helpers;

namespace TG.Backend.Features.Offer.Validation;

public class GetOffersFilterDTOValidator : AbstractValidator<GetOffersQuery>
{
    public GetOffersFilterDTOValidator()
    {
        RuleFor(v => v.Filter.Gearbox)
            .Must(v => v == null || v.BeValidGearbox())
            .WithMessage(@"Valid values for gearbox are: ""Automatic"" or ""Manual""!");
        
        RuleFor(v => v.Filter.Drive)
            .Must(v => v == null || v.BeValidDrive())
            .WithMessage(@"Valid values for gearbox are: ""AWD"", ""RWD"" or ""FWD""!");
    }
}