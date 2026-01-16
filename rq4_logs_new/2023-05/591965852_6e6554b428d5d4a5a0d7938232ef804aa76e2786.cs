using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;

namespace JobSeek.Api.Validations
{
    [AttributeUsage(AttributeTargets.Property)]
    public class EmailValidate :ValidationAttribute
    {
        public override bool IsValid(object? value)
        {
            if (value == null)
                return true;

            Regex regex = new Regex(@"^([\w\.\-]+)@([\w\-]+)((\.(\w){2,3})+)$");
            Match EmailMatch = regex.Match((string)value);
            if (EmailMatch.Success != true)
                return false;

            return true;
        }
    }
}