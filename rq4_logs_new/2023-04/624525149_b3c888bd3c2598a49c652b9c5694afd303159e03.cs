using FluentValidation;

namespace TG.Backend.Features.Auth.Validation
{
    public class AppUserChangePasswordDTOValidator : AbstractValidator<ChangePasswordCommand>
    {
        public AppUserChangePasswordDTOValidator()
        {
            RuleFor(x => x.AppUser.Email)
                .EmailAddress()
                .NotEmpty();
            
            RuleFor(x => x.AppUser.NewPassword)
                .NotEmpty();
            
            RuleFor(x => x.AppUser.Token)
                .NotEmpty();
        }
    }
}