using System;
namespace Taspin.Api.Services
{
    public class ServiceValidator
    {
        public static void ValidateString(string username, string paramName)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                throw new ArgumentException("Parameter was null or white space", paramName);
            }
        }

        public static void ValidateGreaterThanZero(int listToItemId, string paramName)
        {
            if (listToItemId <= 0)
                throw new ArgumentException("Parameter cannot be less or equal zero", paramName);
        }
    }
}