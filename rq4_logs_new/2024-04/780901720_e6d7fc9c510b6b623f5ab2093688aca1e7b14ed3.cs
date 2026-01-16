using FluentValidation;
using TestBuildersPattern.ClassLibrary.Entities;

namespace TestBuildersPattern.ClassLibrary.Validators;
public sealed class ClientValidator : AbstractValidator<Client>
{
    public ClientValidator()
    {
        RuleFor(c => c.Name).Length(3, 150)
            .WithMessage("Name need to have 3 to 150 characters.");

        RuleFor(c => c.Description).Length(3, 1000)
            .WithMessage("Description need to have 3 to 1000 characters.");

        RuleFor(c => c.Author).Length(3, 100)
            .WithMessage("Author need to have 3 to 100 characters.");

        RuleFor(c => c.Age).GreaterThan(0)
            .WithMessage("Age need to be greather than 0.");
    }
}