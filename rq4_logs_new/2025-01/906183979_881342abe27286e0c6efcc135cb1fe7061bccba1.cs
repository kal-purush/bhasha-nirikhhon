using Application.Commands.JobbApplicationCommands;
using FluentValidation;

namespace MyCvSite.Validators
{
    public class CreateJobbApplicationCommandValidator : AbstractValidator<CreateJobbApplicationCommand>
    {
        public CreateJobbApplicationCommandValidator()
        {
            RuleFor(x => x.JobTitle).NotEmpty().WithMessage("Job title is required.");
            RuleFor(x => x.CompanyName).NotEmpty().WithMessage("Company name is required.");
            RuleFor(x => x.ApplicationDate).LessThanOrEqualTo(DateTime.Today).WithMessage("Application date cannot be in the future.");
            RuleFor(x => x.Status).IsInEnum().WithMessage("Invalid status value.");
        }
    }
}