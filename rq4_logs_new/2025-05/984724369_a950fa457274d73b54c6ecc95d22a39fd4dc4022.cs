using DirectoryProject.DirectoryService.Application.Shared.Validation;
using DirectoryProject.DirectoryService.Domain.DepartmentValueObjects;
using DirectoryProject.DirectoryService.Domain.LocationValueObjects;
using DirectoryProject.DirectoryService.Domain.Shared;
using FluentValidation;

namespace DirectoryProject.DirectoryService.Application.DepartmentHandlers.CreateDepartment;

public class CreateDepartmentCommandValidator : AbstractValidator<CreateDepartmentCommand>
{
    public CreateDepartmentCommandValidator()
    {
        {
            RuleFor(c => c.Name)
                .MustBeValueObject(DepartmentName.Create);

            RuleFor(c => c.ParentId)
                .Must(d => d != Guid.Empty)
                .WithError(ErrorHelper.General.ValueIsInvalid("ParentId"));

            RuleFor(c => c.LocationIds)
                .NotEmpty()
                .WithError(ErrorHelper.General.ValueIsNullOrEmpty("LocationIds"));
        }
    }
}