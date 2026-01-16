using CurrencyApi.Base.Response;
using FluentValidation;
using Schema.Request;

namespace Business.Validator;

public class ConvertMultipleCurrencyValidator : AbstractValidator<ConvertMultipleCurrencyRequest>
{
    public ConvertMultipleCurrencyValidator()
    {
        RuleFor(x => x.From)
            .NotEmpty()
            .WithMessage("From Currency is required")
            .MaximumLength(3)
            .WithMessage("From Currency must be 3 characters")
            .Must(receivingCurrency => ISO._4217.CurrencyCodesResolver.Codes.Any(c => c.Code == receivingCurrency))
            .WithMessage("Please enter a valid currency code");
            
        RuleFor(x => x.To)
            .NotEmpty()
            .WithMessage("To Currencies are required")
            .ForEach(currency =>
            {
                currency.MaximumLength(3)
                    .WithMessage("Each To Currency must be 3 characters")
                    .Must(currencyCode => ISO._4217.CurrencyCodesResolver.Codes.Any(c => c.Code == currencyCode))
                    .WithMessage("Please enter a valid currency code");
            });
    }
}