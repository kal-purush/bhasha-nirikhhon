using FluentValidation;
using SixLabors.ImageSharp;

namespace Application.Users.Features.UpdateUserPhoto;

public class UpdateUserPhotoInputValidator : AbstractValidator<UpdateUserPhotoInput>
{
    public UpdateUserPhotoInputValidator()
    {
        RuleFor(x => x.UserPhoto)
            .Cascade(CascadeMode.Stop)
            .Must(BeValid).WithMessage("Given user photo is not valid");
    }

    private bool BeValid(byte[] userPhoto)
    {
        try
        {
            using Image image = Image.Load(userPhoto); ;
            return true;
        }
        catch
        {
            return false;
        }
    }
}