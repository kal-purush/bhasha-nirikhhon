using FastFood_Web.Domain.Entities.Empolyees;
using FastFood_Web.Service.Common.Attributes;
using System.ComponentModel.DataAnnotations;

namespace FastFood_Web.Service.Dtos.AdminDto
{
    public class AdminRegisterDto
    {
        [Required, MaxLength(30), MinLength(5)]
        public string FullName { get; set; } = String.Empty;
        [Required, EmailAttribute]
        public string Email { get; set; } = String.Empty;
        [Required, PasswordAttribute]
        public string Password { get; set; } = String.Empty;

        public static implicit operator Admin(AdminRegisterDto adminRegister)
        {
            return new Admin()
            {
                FullName = adminRegister.FullName,
                Email = adminRegister.Email,
            };
        }
    }
}