using System.ComponentModel.DataAnnotations;

namespace Application.Dtos
{
    public class UserDto
    {
        [Required(ErrorMessage = "Username is required.")]
        [StringLength(400, ErrorMessage = "Username cannot exceed 40 characters.")]
        public string Username { get; set; } = string.Empty;

        [Required(ErrorMessage = "Password is required.")]
        [StringLength(40, ErrorMessage = "Password cannot exceed 40 characters.")]
        public string Password { get; set; } = string.Empty;
    }
}
