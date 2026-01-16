using System;
using System.Collections.Generic;

namespace PalindromeDescendant
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            IsPalindromeTest();
            GetDescendantTest();
            PalindromeDescendantTest();
        }

        public static bool PalindromeDescendant(long value, int decendentToCheck = 2)
        {
            if (IsPalindrome(value))
                return true;

            if (decendentToCheck == 0)
                return false;

            decendentToCheck--;

            try
            {
                long childValue = GetDescendant(value);
                return PalindromeDescendant(childValue, decendentToCheck);
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }
        public static bool IsPalindrome(long value)
        {
            char[] splitIntoArray = value.ToString().Replace("-", string.Empty).ToCharArray();

            if (splitIntoArray.Length == 1)
                return true;

            for (int i = 0; i < splitIntoArray.Length / 2; i++)
            {
                char headValue = splitIntoArray[i];
                char tailValue = splitIntoArray[splitIntoArray.Length - 1 - i];

                if (!headValue.Equals(tailValue))
                    return false;
            }

            return true;
        }
        public static long GetDescendant(long value)
        {
            char[] splitIntoArray = value.ToString().Replace("-", string.Empty).ToCharArray();

            //Number of digits cannot be odd
            if (splitIntoArray.Length % 2 == 1)
                throw new InvalidOperationException();

            int? firstValue = null;
            List<int> descendantValues = new List<int>();

            for (int i = 0; i < splitIntoArray.Length; i++)
            {
                string currentValue = splitIntoArray[i].ToString();

                if (!firstValue.HasValue)
                    firstValue = Convert.ToInt16(currentValue);
                else
                {
                    int sumOfDigit = firstValue.Value + Convert.ToInt16(currentValue);
                    descendantValues.Add(sumOfDigit);
                    firstValue = null;
                }
            }

            return Convert.ToInt64(string.Join("", descendantValues));
        }

        public static void IsPalindromeTest()
        {
            //false
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "13", IsPalindrome(13)));
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "-43", IsPalindrome(-43)));
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "4314", IsPalindrome(4314)));
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "0110", IsPalindrome(0110)));

            //true
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "0", IsPalindrome(0)));
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "1", IsPalindrome(1)));
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "33", IsPalindrome(33)));
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "434", IsPalindrome(434)));
            Console.WriteLine(string.Format("isPalindrome: {0} Result:{1}", "-7325995237", IsPalindrome(-7325995237)));
        }
        public static void GetDescendantTest()
        {
            //Exception
            string result;
            try
            {
                result = GetDescendant(12345).ToString();
            }
            catch (InvalidOperationException)
            {
                result = "InvalidOperationException";
            }

            Console.WriteLine(string.Format("GetDescendant: {0} Result:{1}", "12345", result));

            try
            {
                result = GetDescendant(1).ToString();
            }
            catch (InvalidOperationException)
            {
                result = "InvalidOperationException";
            }

            Console.WriteLine(string.Format("GetDescendant: {0} Result:{1}", "1", result));

            //Output Expected
            Console.WriteLine(string.Format("GetDescendant: {0} Result:{1}", "1234", GetDescendant(1234)));
            Console.WriteLine(string.Format("GetDescendant: {0} Result:{1}", "7898", GetDescendant(7898)));
        }
        public static void PalindromeDescendantTest()
        {
            //false
            Console.WriteLine(string.Format("PalindromeDescendant: {0} Result:{1}", "11211230", PalindromeDescendant(11211230)));

            //true
            Console.WriteLine(string.Format("PalindromeDescendant: {0} Result:{1}", "13001120", PalindromeDescendant(13001120)));
            Console.WriteLine(string.Format("PalindromeDescendant: {0} Result:{1}", "23336014", PalindromeDescendant(23336014)));
            Console.WriteLine(string.Format("PalindromeDescendant: {0} Result:{1}", "11", PalindromeDescendant(11)));
        }
    }
}