using FluentValidation;
using Map.API.Extension;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.AddTravel;

namespace Map.API.Validator.TravelValidator;

public class AddTravelValidator : AbstractValidator<AddTravelDto>
{
    public AddTravelValidator()
    {
        RuleFor(dto => dto.TransportMode)
            //Check if the TransportMode is not empty
            .NotEmpty()
            .WithErrorCode(ETravelErrorCode.TransportModeNotEmpty.ToStringValue())
            .WithMessage("Le mode de transport est requis")
            .Unless(dto => dto is null);

        RuleFor(dto => dto.Distance)
            //Check if the Distance is not empty
            .NotEmpty()
            .WithErrorCode(ETravelErrorCode.DistanceNotEmpty.ToStringValue())
            .WithMessage("La distance pour ce trajet est requise")
            .Unless(dto => dto is null);

        RuleFor(dto => dto.Duration)
            //Check if the DestinationStepId is not empty
            .NotEmpty()
            .WithErrorCode(ETravelErrorCode.DurationNotEmpty.ToStringValue())
            .WithMessage("Le temp hestimer de ce trajet est requis")
            .Unless(dto => dto is null);

        RuleFor(dto => dto.TravelRoad)
            //Check if the DestinationStepId is not empty
            .NotEmpty()
            .WithErrorCode(ETravelErrorCode.TravelRoadNotEmpty.ToStringValue())
            .WithMessage("Les données de la route de ce trajet sont requise")
            .Unless(dto => dto is null);

    }
}