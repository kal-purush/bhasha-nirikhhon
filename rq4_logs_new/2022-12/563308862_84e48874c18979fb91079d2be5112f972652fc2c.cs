using System.ComponentModel.DataAnnotations;
using System.Xml.Linq;

namespace assigMVCPeople.Models.ViewModels
{
    public class RegisterUserViewModel
    {
        [Display(Name = "First Name")]
        [Required]
        [StringLength(120, MinimumLength = 2)]
        public string? FirstName { get; set; }

        [Display(Name = "Last Name")]
        [Required]
        [StringLength(120, MinimumLength = 2)]
        public string? LastName { get; set; }

        [Required]
        [Display(Name = "Date of Brith")]
        [StringLength(120, MinimumLength = 2)]
        public DateTime DateOfBirth { get; set; }

        [Required]
        [Display(Name ="User Name")]
        [StringLength(60, MinimumLength = 4)]
        public string? UserName { get; set; }

        [Required]
        [Display(Name = "Email")]
        [DataType(DataType.EmailAddress)]
        [StringLength(60, MinimumLength = 4)]
        public string? Email { get; set; }

        [Required]
        [Display(Name = "Password")]
        [DataType(DataType.Password)]
        [StringLength(80, MinimumLength = 6)]
        public string? Password { get; set; }

        [Required]
        [Display(Name = "Confirm Password")]
        [DataType(DataType.Password)]
        [Compare("Password")]
        public string? ConfirmPassword { get; set; }
    }
}