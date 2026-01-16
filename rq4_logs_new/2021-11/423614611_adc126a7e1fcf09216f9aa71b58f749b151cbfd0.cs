using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace church_mgt_dtos.AuthenticationDtos
{
    public class ResetPasswordDto
    {
        public string Token { get; set; }

        public string Email { get; set; }

        public string NewPassword { get; set; }

        [Compare("NewPassword", ErrorMessage = "New Password and Confirm Password must match.")]
        public string ConfirmPassword { get; set; }
    }
}