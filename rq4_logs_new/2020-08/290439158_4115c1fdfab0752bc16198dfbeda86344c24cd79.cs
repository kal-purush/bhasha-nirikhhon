using System;
using System.Collections.Generic;
using System.Text;


namespace TimeManagementBooking
{
    public class TimeBProcessor : ITimeBProcessor
    {
  /*       private readonly IBookingProcessor _bookingProcessor;

        public TimeBProcessor(IBookingProcessor bookingprocessor)
        {
            _bookingProcessor= bookingprocessor;
        }
 */
        public bool BookTime(Employee employee, DateTime date, decimal duration)
        {
            if(employee.empId <= 0)
            {
                throw new ArgumentOutOfRangeException("Employee id cannot be 0");
            }

            if(date.Date < DateTime.Today || date.Date > DateTime.Today)
            {
                throw new ArgumentOutOfRangeException("You can book for today only.");
            }

            if(duration > 9)
            {
                throw new ArgumentOutOfRangeException("You can book for 9hrs only");
            }

            return true;

            //return _bookingProcessor.Create(employee.empId,date,duration);
        }
    }
}