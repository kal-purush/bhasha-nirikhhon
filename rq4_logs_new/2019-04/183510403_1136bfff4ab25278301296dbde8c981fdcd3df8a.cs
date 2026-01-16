using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Trivago.Classes
{
    static class HelperClass
    {
        public static bool ValidEmail(string Email)
        {
            try
            {
                var addr = new System.Net.Mail.MailAddress(Email);
                return addr.Address == Email;
            }
            catch
            {
                return false;
            }
        }
        
        public static bool IsDigitsOnly(string str)
        {
            foreach (char c in str)
            {
                if (c < '0' || c > '9')
                    return false;
            }

            return true;
        }
        public static bool ValidRating(int HotelRating)
        {
            if (HotelRating > 0 && HotelRating <= 5)
                return true;
            else return false;
        }
        public static  bool ValidPhoneNumber(string Number)
        {
            if (HelperClass.IsDigitsOnly(Number) && Number.Length == 12)
            {
                return true;
            }
            return false;
        }
    }
}