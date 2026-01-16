using FluentValidation;
using SecretCode.Api.Features.User.Commands;

namespace SecretCode.Api.Features.User.Validators;

public class CreateUserCommandValidator : AbstractValidator<CreateUserCommand>
{
    public CreateUserCommandValidator()
    {
        RuleFor(command => command.Name)
            .NotEmpty()
            .NotNull()
            .WithMessage("Name not provided");
        
        RuleFor(command => command.Email)
            .NotEmpty()
            .NotNull()
            .WithMessage("Email not provided");
    }
}