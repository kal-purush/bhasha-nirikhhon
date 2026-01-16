using FluentValidation;

namespace Application.Common.Validators
{
    internal class PaginationParamsValidator : AbstractValidator<PaginationParams>
    {
        public PaginationParamsValidator()
        {
            RuleFor(x => x.Page)
                .GreaterThan(0).WithMessage("{PropertyName} must be greater than 0.");

            RuleFor(x => x.PageSize)
                .GreaterThan(0).WithMessage("{PropertyName} must be greater than 0.");

            RuleFor(x => x.OrderBy)
                .Must(x => string.IsNullOrEmpty(x) || x == "asc" || x == "desc")
                .WithMessage("{PropertyName} must be either 'asc' or 'desc' if specified.");

            RuleFor(x => x.OrderByColumn)
                .Must(x => string.IsNullOrEmpty(x) || x == "Name" || x == "Price")
                .WithMessage("{PropertyName} must be either 'Name' or 'Price' if specified.");
        }
    }
}