using System.Text.RegularExpressions;

namespace HotelReservation
{
    public static partial class Handler
    {
        public static class Validator
        {
            private static readonly string EmailPattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";
            private static readonly string PhonePattern = @"^\+?[1-9]\d{1,14}$";

            public static bool ValidateEmail(string email)
            {
                if (string.IsNullOrWhiteSpace(email))
                {
                    return false;
                }

                return Regex.IsMatch(email, EmailPattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
            }

            public static bool ValidatePhoneNumber(string phoneNumber)
            {
                if (string.IsNullOrWhiteSpace(phoneNumber))
                {
                    return false;
                }

                return Regex.IsMatch(phoneNumber, PhonePattern);
            }
        }
    }
}