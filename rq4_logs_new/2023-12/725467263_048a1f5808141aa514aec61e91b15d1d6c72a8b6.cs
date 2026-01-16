using System.ComponentModel.DataAnnotations;

namespace toiec_web.ViewModels.Authentication
{
    public class ChangePasswordModel
    {
        [Required(ErrorMessage = "Username is reqired")]
        public string Username { get; set; }

        [Required(ErrorMessage = "Current password is reqired")]
        public string CurrentPassword { get; set; }

        [Required(ErrorMessage = "New password is reqired")]
        public string NewPassword { get; set; }

        [Required(ErrorMessage = "Confirm password is reqired")]
        [Compare("NewPassword", ErrorMessage = "The new password and confirmpassword do not match")]
        public string ConfirmPassword { get; set; }
    }
}