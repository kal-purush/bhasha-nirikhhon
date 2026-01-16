namespace ApiaryManagementSystem.Application.Features.BeeQueens.Queries.GetBeeQueens;

using FluentValidation;
using static ApiaryManagementSystem.Application.Common.Constants.ApplicationConstants.Pagination;

public class GetBeeQueensQueryValidator : AbstractValidator<GetBeeQueensQuery>
{
    public GetBeeQueensQueryValidator()
    {
        RuleFor(x => x.Page)
            .GreaterThanOrEqualTo(MinPage)
                .WithMessage($"Page should be greater than or equal to {MinPage}.");

        RuleFor(x => x.PageSize)
            .GreaterThanOrEqualTo(MinPageSize)
                .WithMessage($"Page size should be greater than or equal to {MinPageSize}.")
            .LessThanOrEqualTo(MaxPageSize)
                .WithMessage($"Page size should be less than or equal to {MaxPageSize}.");
    }
}