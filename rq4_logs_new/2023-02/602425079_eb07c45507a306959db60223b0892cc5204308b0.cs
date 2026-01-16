using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using System.ComponentModel.DataAnnotations;

namespace StudentAttandance.Pages.User
{
    public class ChangePasswordModel : PageModel
    {
        [Required(ErrorMessage = "Password is required")]
        [MinLength(5, ErrorMessage = "Password must be at least 5 characters long")]
        [BindProperty]
        public string OldPassword { get; set; }
        [Required(ErrorMessage = "Password is required")]
        [MinLength(5, ErrorMessage = "Password must be at least 5 characters long")]
        [BindProperty]
        public string NewPassword { get; set; }
        [Required(ErrorMessage = "Password is required")]
        [MinLength(5, ErrorMessage = "Password must be at least 5 characters long")]
        [ConfirmPassword("NewPassword", ErrorMessage = "The new password and confirm password do not match.")]
        [BindProperty]
        public string ConfirmPassword { get; set; }
        public void OnGet()
        {
        }

        public IActionResult OnPost()
        {
            if (ModelState.IsValid)
            {

            }
            return Page();
        }
    }


    class ConfirmPasswordAttribute : ValidationAttribute
    {
        private readonly string _passwordPropertyName;

        public ConfirmPasswordAttribute(string passwordPropertyName)
        {
            _passwordPropertyName = passwordPropertyName;
        }

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var passwordProperty = validationContext.ObjectType.GetProperty(_passwordPropertyName);
            if (passwordProperty == null)
            {
                return new ValidationResult($"Unknown property {_passwordPropertyName}");
            }

            var passwordValue = passwordProperty.GetValue(validationContext.ObjectInstance, null) as string;
            var confirmationValue = value as string;

            if (passwordValue != confirmationValue)
            {
                return new ValidationResult("The password and confirmation password do not match.");
            }

            return ValidationResult.Success;
        }
    }
}