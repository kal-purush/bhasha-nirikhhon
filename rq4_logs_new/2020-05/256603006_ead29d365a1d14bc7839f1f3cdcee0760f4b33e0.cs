using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Giraffe
{
    public class IfStatementPractice
    {
        /// <summary>
        /// Create a method that checks what day of the week it is.
        /// If today's date(day) is NOT Sunday, print to console:
        /// "I will do my morning daily exercises."
        /// Otherwise, if today's date(day) = Sunday, then print to console:
        /// "Hot diggity, I get to do an easy yoga class instead!"
        /// </summary>
        /// <returns></returns>
        public string IfNotSundayThenWhat()
        {
            //Get today's date (day)
            DateTime dayToday = DateTime.Today;
            //Assign day as today's date in dddd format (just the day of the week)
            DayOfWeek day = DateTime.Now.DayOfWeek;

            //string notSunday = "Today is " + day + ", I will do my morning daily exercises.";
            //string isSunday = "Today is " + day + ", hot diggity, I get to do an easy yoga class instead!";

            if (day != DayOfWeek.Sunday)
            {
                string whatDayIsIt = "Today is " + day + ", I will do my morning daily exercises.";
                return whatDayIsIt;
            }
            else
            {
                string whatDayIsIt = "Today is " + day + ", hot diggity, I get to do an easy yoga class instead!";
                return whatDayIsIt;
            }

        }
    }
}