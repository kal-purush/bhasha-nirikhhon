namespace ApiaryManagementSystem.Application.Features.BeeQueens.Commands.CreateBeeQueen;

using FluentValidation;

using static Domain.Constants.Models.BeeQueen;

public class CreateBeeQueenCommandValidator : AbstractValidator<CreateBeeQueenCommand>
{
    public CreateBeeQueenCommandValidator()
    {
        RuleFor(x => x.Year)
            .GreaterThanOrEqualTo(1900)
            .LessThanOrEqualTo(9999)
            .NotEmpty();

        RuleFor(x => x.ColorMark)
            .MaximumLength(ColorMarkMaxLength);

        RuleFor(x => x.HiveId)
            .NotEmpty();
    }
}