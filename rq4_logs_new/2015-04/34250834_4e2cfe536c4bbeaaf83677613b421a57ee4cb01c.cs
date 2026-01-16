using System;
using System.Linq;

namespace B15_Ex01_5
{
    public class Program
    {
        static int s_NumbersGreaterThanFirst = 0;
        static int s_NumbersSmallerThanFirst = 0;
        
        public static void Main()
        {

            string inputFromUser = getInput();
            countGreaterAndSmallerNumbers(inputFromUser);

            char minValue = inputFromUser.Min();
            char maxValue = inputFromUser.Max();


            Console.WriteLine("Number Greater than 1st: {0} ", s_NumbersGreaterThanFirst);
            Console.WriteLine("Number Smaller than 1st: {0} ", s_NumbersSmallerThanFirst);
            Console.WriteLine("The min value is       : {0} ", minValue);
            Console.WriteLine("The max value is       : {0} ", maxValue);
            Console.ReadLine();
        }

        //get input from the user
        private static string getInput(){
            string inputFromUser;
            while (true)
            {
                Console.WriteLine("Please enter 8 digit string: ");
                inputFromUser = Console.ReadLine();
                if (isValidString(inputFromUser))
                {
                    break;
                }
                Console.WriteLine("Invalid input! please try again...");
            }
            return inputFromUser;
        }

        //check if valid string was given
        private static bool isValidString(string i_StringToCheck)
        {
            return i_StringToCheck.Length == 8 && containsOnlyNumbers(i_StringToCheck);
        }
        //check if cotains only numbers
        private static bool containsOnlyNumbers(string i_StringToCheck)
        {
            bool isNumber = false;
            for (int i = 0; i < i_StringToCheck.Length; i++)
            {
                isNumber = Char.IsNumber(i_StringToCheck[i]);
            }
            return isNumber;
        }

        //count how many numbers greater than first number
        private static void countGreaterAndSmallerNumbers(string i_StringOfNumbers)
        {
            int numberToCompare;
            int firstNumber = getNumValue(i_StringOfNumbers[0]);

            for (int i = 1; i < i_StringOfNumbers.Length; i++)
            {
                numberToCompare = getNumValue(i_StringOfNumbers[i]);
                
         //       Console.WriteLine("THE EQ IS : {0} < {1}", numberToCompare, i_StringOfNumbers[0]);
                if (numberToCompare > firstNumber)
                {
                      s_NumbersGreaterThanFirst++;
                }
                else if (numberToCompare < firstNumber)
                {
                      s_NumbersSmallerThanFirst++;
                }
                
            }
        }

        //get numeric value from char
        private static int getNumValue(char i_CharToNumericValue)
        {
            double numericValue = Char.GetNumericValue(i_CharToNumericValue);
            return Convert.ToInt32(numericValue);
        }
        
    }
}