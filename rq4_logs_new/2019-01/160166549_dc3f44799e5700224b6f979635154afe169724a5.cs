using Common.Constant;
using System.ComponentModel.DataAnnotations;

namespace Common.ModelsDTO
{
    public class UpdatePasswordDTO
    {
        [Required]
        public string PasswordOld { get; set; }

        [Required]
        [MinLength(8)]
        [RegularExpression(Constants.PasswordRegularExpression, ErrorMessage = Constants.PasswordInvalidMessage)]
        public string PasswordNew { get; set; }

        [Required]
        [Compare("PasswordNew")]
        public string PasswordConfirm { get; set; }
    }
}