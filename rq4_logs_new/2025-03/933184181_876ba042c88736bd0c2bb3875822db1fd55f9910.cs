using FluentValidation;
using TaskFlow.SprintService;

public class CreateSprintValidator : AbstractValidator<CreateSprintReq>
{
    public CreateSprintValidator()
    {
        RuleFor(x => x)
            .NotNull().WithMessage("Sprint request cannot be null.");

        RuleFor(x => x.Name)
            .NotEmpty().WithMessage("Sprint name is required")
            .MaximumLength(100).WithMessage("Sprint name cannot exceed 100 characters");

        RuleFor(x => x.DateStarted)
            .NotEmpty().WithMessage("Date started is required");

        RuleFor(x => x.DateEnded)
            .NotEmpty().WithMessage("Date ended is required");

        RuleFor(x => x.ProjectId)
            .NotEmpty().WithMessage("Project ID is required");
    }
}


public class UpdateSprintValidator : AbstractValidator<UpdateSprintReq>
{
    public UpdateSprintValidator()
    {
        RuleFor(x => x.Name)
            .NotEmpty().WithMessage("Sprint name is required")
            .MaximumLength(100).WithMessage("Project name cannot exceed 100 characters");
        RuleFor(x => x.DateStarted)
            .NotEmpty().WithMessage("Date started is required");    
        RuleFor(x => x.DateEnded)
            .NotEmpty().WithMessage("Date ended is required");
    }
}