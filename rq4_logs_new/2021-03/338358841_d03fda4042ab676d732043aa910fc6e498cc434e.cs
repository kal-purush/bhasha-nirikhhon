using System;

namespace FlightClasses
{
    public class clsOrderline
    {
        public clsOrderline()
        {
        }

        public int OrderlineID { get; set; }
        public string BookingNo { get; set; }
        public int Quantity { get; set; }
        public int BookingID { get; set; }

        public string Valid(string someBookingNo, string someQuantity)
        {
            //string variable to store the error message
            string Error = "";

            //if the  BookingNo is more than 30 characters
            if (someBookingNo.Length > 5)
            {
                //return an error message
                Error = Error + "The staff email cannot have more than 5 characters";
            }
            if (someBookingNo.Length == 0 | someBookingNo.Length < 1)
            {
                //return an error message
                Error = Error + "The  Staff Email may not be blank";
            }

            //if the  BookingNo is more than 30 characters
            if (someQuantity.Length > 5)
            {
                //return an error message
                Error = Error + "The staff email cannot have more than 5 characters";
            }
            if (someQuantity.Length == 0 | someQuantity.Length < 1)
            {
                //return an error message
                Error = Error + "The  Staff Email may not be blank";
            }
            return Error;
        }
    }
}