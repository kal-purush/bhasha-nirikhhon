using FluentValidation;
using TaskFlow.ProjectService;

public class CreateProjectValidator : AbstractValidator<CreateProjectReq>
{
    public CreateProjectValidator()
    {
        RuleFor(x => x.Name)
            .NotEmpty().WithMessage("Project name is required")
            .MaximumLength(100).WithMessage("Project name cannot exceed 100 characters");

        RuleFor(x => x.Description)
            .NotEmpty().WithMessage("Description is required");

        RuleFor(x => x.OwnerId)
            .NotEmpty().WithMessage("Owner ID is required");
    }
}