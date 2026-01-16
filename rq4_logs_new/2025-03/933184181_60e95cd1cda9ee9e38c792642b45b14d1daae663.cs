using FluentValidation;
using TaskFlow.ProjectService;

public class ListProjectsValidator : AbstractValidator<ListProjectsReq>
{
    public ListProjectsValidator()
    {
        RuleFor(x => x.Page)
            .GreaterThanOrEqualTo(1).WithMessage("Page must be greater than or equal to 1");

        RuleFor(x => x.Limit)
            .GreaterThanOrEqualTo(1).WithMessage("Limit must be greater than or equal to 1")
            .LessThanOrEqualTo(100).WithMessage("Limit cannot exceed 100 items per page");
    }
}