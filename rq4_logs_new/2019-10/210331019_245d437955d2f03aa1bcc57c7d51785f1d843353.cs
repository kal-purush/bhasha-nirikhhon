using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace Gride.Models
{
    public class EndDateValidator : ValidationAttribute
    {
        private DateTime _start;

        private int day;


        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            Availability timeslot = (Availability)validationContext.ObjectInstance;
            var end = ((DateTime)value);

            _start = timeslot.Start;
            day = (int)_start.DayOfWeek;

            int endDay = (int)end.DayOfWeek;

            if (end <= _start)
            {
                return new ValidationResult(GetErrorMessage1());
            }

            if (day != endDay)
            {
                return new ValidationResult(GetErrorMessage2());
            }

            return ValidationResult.Success;
        }

        public string GetErrorMessage1()
        {
            return "The end date can't be before the start date.";
        }
        public string GetErrorMessage2()
        {
            return "The end date can't be on a different day than the start date. Please make two different availabilities.";
        }
    }
}