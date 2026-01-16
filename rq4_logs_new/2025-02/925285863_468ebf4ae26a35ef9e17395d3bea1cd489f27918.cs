using FluentValidation;
using WebApplicationCourseNTier.Business.DTOs.User;

namespace WebApplicationCourseNTier.Business.Validators
{
    public class RegisterUserDtoValidator : AbstractValidator<RegisterUserDto>
    {
        public RegisterUserDtoValidator()
        {
            // UserName üçün validasiya
            RuleFor(x => x.UserName)
                .NotEmpty().WithMessage("Username is required.")
                .Length(1, 50).WithMessage("Username cannot exceed 50 characters.");

            // Password üçün validasiya
            RuleFor(x => x.Password)
                .NotEmpty().WithMessage("Password is required.")
                .MinimumLength(8).WithMessage("Password must be at least 8 characters.");

            // ConfirmPassword üçün validasiya
            RuleFor(x => x.ConfirmPassword)
                .NotEmpty().WithMessage("Confirm Password is required.")
                .Equal(x => x.Password).WithMessage("Password and Confirm Password do not match.");

            // Email üçün validasiya
            RuleFor(x => x.Email)
                .NotEmpty().WithMessage("Email is required.")
                .EmailAddress().WithMessage("Invalid email format.")
                .MaximumLength(100).WithMessage("Email cannot exceed 100 characters.");

            // Phone üçün validasiya
            RuleFor(x => x.Phone)
                .NotEmpty().WithMessage("Phone number is required.")
                .Matches(@"^\+?[0-9]{7,15}$").WithMessage("Invalid phone number format.");
        }
    }
}