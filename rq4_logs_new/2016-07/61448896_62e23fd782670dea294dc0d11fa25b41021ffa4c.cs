namespace BudgetTourist.Common
{
    using System;
    public static class Validator
    {
        public static void CheckIfNull(object obj, string message = null)  // collections validator 
        {
            if (obj == null)
            {
                throw new NullReferenceException(message);
            }
        }

        public static void CheckIfStringIsNullOrEmpty(string text, string message = null)  // strings validator 
        {
            if (string.IsNullOrEmpty(text))
            {
                throw new NullReferenceException(message);
            }
        }

        public static void CheckIfLessThenZero(decimal number, string message = null)   // numbers validator
        {
            if (number < 0)
            {
                throw new ArgumentException(message);
            }
        }
    }
}