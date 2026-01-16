using System.Globalization;
using System.Windows.Controls;

namespace DAN_LI.Validation
{
    class ValidAccount:ValidationRule
    {
        public override ValidationResult Validate(object value, CultureInfo cultureInfo)
        {
            string account = value as string;
            
            if (Service.Service.UsedAccount(account))
            {
                return new ValidationResult(false, "This account is already taken");
            }
            else
            {
                return new ValidationResult(true, null);
            }
        }
    }
}