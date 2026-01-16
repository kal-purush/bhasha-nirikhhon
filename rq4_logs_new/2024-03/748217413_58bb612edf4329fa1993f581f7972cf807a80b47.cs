using FluentValidation;
using Map.API.Extension;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.Step;

namespace Map.API.Validator.StepValidator;

internal class UpdateStepTransportModeValidatior : AbstractValidator<UpdateStepTransportModeDto>
{
    public UpdateStepTransportModeValidatior()
    {
        RuleFor(dto => dto)
            //check if the dto is not empty
            .NotEmpty()
            .WithErrorCode(EStepErrorCodes.DtoNotNull.ToStringValue())
            .WithMessage("Le Dto est requis");

        RuleFor(dto => dto.TransportMode)
            //check if the transport mode is not empty
            .NotEmpty()
            .WithErrorCode(EStepErrorCodes.TransportModeNotEmpty.ToStringValue())
            .WithMessage("Le mode de transport est requis");
    }
}