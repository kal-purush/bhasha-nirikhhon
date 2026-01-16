using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IdentitySkillUp.Models
{
    public class RegisterAuthenticatorModel
    {
        [Required]
        public string Code { get; set; }
        [Required]
        public string AuthenticatorKey { get; set; }
    }
}