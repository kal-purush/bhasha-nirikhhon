using FluentValidation;
using Map.API.Extension;
using Map.Domain.Entities;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.User;
using Microsoft.AspNetCore.Identity;

namespace Map.API.Validator.UserValidator;

internal class UpdateUserNameValidator : AbstractValidator<UpdateUserNameDto>
{
    public UpdateUserNameValidator(UserManager<MapUser> userManager)
    {
        if (userManager is null)
            throw new ArgumentNullException(nameof(userManager));

        RuleFor(dto => dto)
            .NotEmpty()
            .WithErrorCode(EMapUserErrorCodes.DtoNotNull.ToStringValue())
            .WithMessage("Le dto est requis");

        #region UserName
        RuleFor(dto => dto.UserName)
            //Check if the username is not empty
            .NotEmpty()
            .WithErrorCode(EMapUserErrorCodes.UserNameNotEmpty.ToStringValue())
            .WithMessage("Le nom de l'utilisateur est requis");
        #endregion
    }
}