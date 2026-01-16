using FluentValidation;
using PetFamily.Application.Extensions;
using PetFamily.Domain.PetContext.ValueObjects.PetVO;
using PetFamily.Domain.Shared.CustomErrors;
using PetFamily.Domain.Shared.SharedVO;

namespace PetFamily.Application.Volunteers.Commands.UpdatePetStatus;

public class UpdatePetHelpStatusCommandValidator : AbstractValidator<UpdatePetHelpStatusCommand>
{
    public UpdatePetHelpStatusCommandValidator()
    {
        RuleFor(c => c.HelpStatus).MustBeValueObject(HelpStatus.Create);
    }
}