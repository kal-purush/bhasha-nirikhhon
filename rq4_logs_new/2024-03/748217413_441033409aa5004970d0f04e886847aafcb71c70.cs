using FluentValidation;
using Map.API.Extension;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.Step;

namespace Map.API.Validator.StepValidator;

public class UpdateStepDateValidator : AbstractValidator<UpdateStepDateDto>
{
    public UpdateStepDateValidator()
    {
        RuleFor(dto => dto)
            //check if the dto is not empty
            .NotEmpty()
            .WithErrorCode(EStepErrorCodes.DtoNotNull.ToStringValue())
            .WithMessage("Le Dto est requis");

        RuleFor(dto => dto.StartDate)
            //check if the start date is not empty
            .NotEmpty()
            .WithErrorCode(EStepErrorCodes.StartDateNotEmpty.ToStringValue())
            .WithMessage("La date de début est obligatoire");

        RuleFor(dto => dto.EndDate)
            //check if the end date is not empty
            .NotEmpty()
            .WithErrorCode(EStepErrorCodes.EndDateNotEmpty.ToStringValue())
            .WithMessage("La date de fin est obligatoire");
    }
}