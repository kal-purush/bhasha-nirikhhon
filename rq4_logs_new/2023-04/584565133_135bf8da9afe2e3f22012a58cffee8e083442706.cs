using Colabora.Application.Features.SocialAction.CreateSocialAction.Models;
using Colabora.Domain.Enums;
using FluentValidation;

namespace Colabora.Application.Validation;

public class CreateSocialActionValidator : AbstractValidator<CreateSocialActionCommand>
{
    public CreateSocialActionValidator()
    {
        RuleFor(command => command.Title)
            .NotEmpty()
            .MinimumLength(3)
            .MaximumLength(100);

        RuleFor(command => command.Description)
            .NotEmpty()
            .MinimumLength(10)
            .MaximumLength(255);

        RuleFor(command => command.State)
            .NotEqual(States.Undefined);

        RuleFor(command => command.Interests)
            .NotEmpty()
            .Must(list => !list.Contains(Interests.Undefined));

        RuleFor(command => command.OccurrenceDate)
            .GreaterThan(DateTimeOffset.Now);

        RuleFor(command => command.OrganizationId)
            .GreaterThan(0);

        RuleFor(command => command.VolunteerCreatorId)
            .GreaterThan(0);
    }
}