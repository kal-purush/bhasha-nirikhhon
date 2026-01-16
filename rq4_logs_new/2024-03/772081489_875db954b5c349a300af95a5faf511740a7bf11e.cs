using System.ComponentModel.DataAnnotations;

namespace Examination.ViewModel
{
    public class EditProfileViewModel
    {
        public string email { get; set; }

        [Required]
        [DataType(DataType.Password)]
        public string oldPass { get; set; }


        [DataType(DataType.Password)]
        [Required(ErrorMessage = "Please enter your new password.")]
        [StringLength(20, MinimumLength = 8)]
        [RegularExpression(@"^(?=.*?[A-Za-z])(?=.*?[0-9])(?=.*?[^A-Za-z0-9]).{8,}$")]
        public string newPass { get; set; }

        [DataType(DataType.Password)]
        [Required(ErrorMessage = "Please confirm your new password.")]
        [Compare("newPass", ErrorMessage = "The new password and confirmation password do not match.")]
        public string conFirmPass { get; set; }

    }
}