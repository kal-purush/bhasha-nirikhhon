using Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace TemperatureRetreiver.Validations
{
    public class DateValidation : IDateValidation
    {
        public bool IsValid(string inputDate)
        {
            DateTime dDate;

            if (DateTime.TryParse(inputDate, out dDate))
            {
                string.Format(Constants.DateFormat, dDate);
                return true;
            }
            else
            {
                Console.WriteLine("Date is not valid");
                return false;
            }
        }
    }
}