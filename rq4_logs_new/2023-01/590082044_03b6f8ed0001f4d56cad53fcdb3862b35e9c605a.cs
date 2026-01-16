using CraftHouse.Web.Entities;
using FluentValidation;

namespace CraftHouse.Web.Validators;

public class UserValidator : AbstractValidator<User>
{
    public UserValidator()
    {
        RuleFor(user => user.FirstName).NotEmpty().Matches("^[A-Z][a-z]{2,255}$");
        RuleFor(user => user.LastName).NotEmpty().Matches("^[A-Z][a-z]{2,255}$");
        RuleFor(user => user.TelephoneNumber).NotEmpty().Matches("^[0-9]{9}$");
        RuleFor(user => user.City).NotEmpty().Matches("^[A-Z][a-z]{2,255}$");
        RuleFor(user => user.PostalCode).NotEmpty().Matches("^[0-9]{2}[-][0-9]{3}$");
        RuleFor(user => user.AddressLine).Must(BeAValidAddressLine);
        RuleFor(user => user.Email).NotEmpty().EmailAddress();
    }

    private static bool BeAValidAddressLine(string? addressLine)
    {
        if (addressLine == null)
        {
            return true;
        }

        if (addressLine.Length >= 3 && char.IsUpper(addressLine[0]))
        {
            return true;
        }

        return false;
    }
}