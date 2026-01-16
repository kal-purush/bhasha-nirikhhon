using FluentValidation;
using PocketForzaHorizonCommunity.Back.DTO.Requests.Car;

namespace PocketForzaHorizonCommunity.Back.DTO.Validation.Car;

public class CreateCarRequestValidation : AbstractValidator<CreateCarRequest>
{
    public CreateCarRequestValidation()
    {
        RuleFor(x => x.Model)
            .NotEmpty()
            .MaximumLength(64);

        RuleFor(x => x.Year)
            .NotEmpty()
            .GreaterThanOrEqualTo(1900)
            .LessThanOrEqualTo(2100);

        RuleFor(x => x.Price)
            .NotEmpty()
            .GreaterThanOrEqualTo(5000)
            .LessThanOrEqualTo(1_000_000_000);

        RuleFor(x => x.Manufacture)
            .NotEmpty()
            .MaximumLength(64);

        RuleFor(x => x.CarType)
            .NotEmpty()
            .MaximumLength(64);
    }
}