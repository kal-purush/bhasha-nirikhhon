using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;

namespace Project.ValidationFarmework.helper
{
    public class ValidationHelper
    {
        //email validation plain version
        public static bool IsValidEmail(string email)
        {
            var trimmedEmail=email.Trim();
            if (email.Length == 0)
            {
                return false;
            }
            if (trimmedEmail.EndsWith("."))
            {
                return false;
            }
            try
            {
                var addr=new System.Net.Mail.MailAddress(email);
                return addr.Address == trimmedEmail;
            }
            catch
            {
                return false;
            }
        }

        //email validation using data-anotation
        public bool IsValidEmailVersion2(string email)
        {
            return new EmailAddressAttribute().IsValid(email);
        }
    }
}