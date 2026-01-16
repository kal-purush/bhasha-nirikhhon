using DirectoryProject.DirectoryService.Application.Shared.DTOs;
using DirectoryProject.DirectoryService.Application.Shared.Validation;
using DirectoryProject.DirectoryService.Domain.LocationValueObjects;
using DirectoryProject.DirectoryService.Domain.Shared;
using FluentValidation;

namespace DirectoryProject.DirectoryService.Application.LocationHandlers.UpdateLocation;
internal class UpdateLocationCommandValidator : AbstractValidator<UpdateLocationCommand>
{
    public UpdateLocationCommandValidator()
    {
        RuleFor(c => c.Id)
            .NotEmpty()
            .Must(d => d != Guid.Empty)
            .WithError(ErrorHelper.General.ValueIsNullOrEmpty("Id"));

        RuleFor(c => c.Name)
            .MustBeValueObject(LocationName.Create);

        RuleFor(c => c.Address)
            .MustBeValueObject(AddressFromDTO);

        RuleFor(c => c.TimeZone)
            .MustBeValueObject(IANATimeZone.Create);
    }

    private static Result<LocationAddress> AddressFromDTO(AddressDTO dto)
        => LocationAddress.Create(dto.City, dto.Street, dto.HouseNumber);
}