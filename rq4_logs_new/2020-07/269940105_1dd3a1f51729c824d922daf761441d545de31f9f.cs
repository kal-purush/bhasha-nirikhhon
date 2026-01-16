using OrganisationArchive.DAL.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace OrganisationArchive.DAL.CustomValidations
{
    [AttributeUsage(AttributeTargets.Property)]
    public class CorrectDate : ValidationAttribute
    {
       // public bool ValidateStartDate { get; set; }
       // public bool ValidateEndDate { get; set; }

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var member = validationContext.ObjectInstance as Person;

            if (member != null)
            {
                if (member.DateOfBirth > DateTime.Now.Date)
                {
                    return new ValidationResult("Date of Birth must not be greater than current date");
                }
            }

            return ValidationResult.Success;
        }
    }
}