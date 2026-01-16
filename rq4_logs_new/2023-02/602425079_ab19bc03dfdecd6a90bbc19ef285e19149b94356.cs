using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace StudentAttandanceLibrary.Validation.CustomValidation
{
    public class IsStringAttribute
    {
    }

    public class IsLetterAttribute : ValidationAttribute
    {
        public IsLetterAttribute()
        {
        }

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (value != null)
            {
                var s = value as string;

                if (!Regex.IsMatch(s, @"^[a-zA-Z ]+$"))
                {
                    return new ValidationResult("Input must is letter.");
                }
            }

            return ValidationResult.Success;
        }
    }
}