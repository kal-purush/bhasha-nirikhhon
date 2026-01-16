using DirectoryProject.DirectoryService.Application.Shared.Validation;
using DirectoryProject.DirectoryService.Domain.DepartmentValueObjects;
using DirectoryProject.DirectoryService.Domain.Shared;
using FluentValidation;

namespace DirectoryProject.DirectoryService.Application.DepartmentHandlers.UpdateDepartment;

public class UpdateDepartmentCommandValidator : AbstractValidator<UpdateDepartmentCommand>
{
    public UpdateDepartmentCommandValidator()
    {
        RuleFor(c => c.Id)
            .NotEmpty()
            .Must(d => d != Guid.Empty)
            .WithError(ErrorHelper.General.ValueIsNullOrEmpty("Id"));

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