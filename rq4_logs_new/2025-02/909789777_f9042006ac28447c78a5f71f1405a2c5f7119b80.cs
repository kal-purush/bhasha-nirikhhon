using FluentValidation;
using JetBrains.Annotations;

namespace PetFamily.Application.Volunteers.HardDelete;

[UsedImplicitly]
public sealed class HardDeleteVolunteerRequestValidator : AbstractValidator<HardDeleteVolunteerRequest>
{
    public HardDeleteVolunteerRequestValidator()
    {
        RuleFor(d => d.VolunteerId).NotEmpty();
    }
}