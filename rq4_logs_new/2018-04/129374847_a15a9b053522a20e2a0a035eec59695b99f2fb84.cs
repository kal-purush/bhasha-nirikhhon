#region Hometask_01_2_3
//
// Author: Shemet Viktoriya
// Description: The user enters month number
// if month 1,2,12 display "Winter"; 3,4,5 - "Spring";
// 6-8 - "Summer"; 9-11 - "Autumn". In any other case - 
// "There is no such month on this planet"
//
#endregion
using System;
namespace Hometask_01_2_3
{
    enum Season
    {
        Winter,
        Spring,
        Summer,
        Autumn,
    }
    class Program
    {
        /// <summary>
        /// This Method identify season by the entered month
        /// </summary>
        /// <param name="numberOfMonth"></param>
        /// <returns>Remainder of division by 12</returns>
        static byte IdentifySeason(byte numberOfMonth)
        {
            if (numberOfMonth >= 1 && numberOfMonth <= 12)
                return (byte)((numberOfMonth + 12) % 12);
            return byte.MaxValue;
        }

        /// <summary>
        /// Read user input and return number of month in right case  
        /// </summary>
        /// <returns>number of month or byte.MaxValue</returns>
        static byte ReadMonth()
        {
            string enter;
            Console.Write("Enter month number: ");
            enter = Console.ReadLine();
            if (byte.TryParse(enter, out byte numberMonth))
            {
                return byte.Parse(enter);
            }
            return byte.MaxValue;
        }

        /// <summary>
        /// This Method Display season by the entered month
        /// </summary>
        /// <param name="numberOfMonth"></param>
        static void DisplaySeason(byte numberOfMonth)
        {
            switch (IdentifySeason(numberOfMonth))
            {
                case 0:
                case 1:
                case 2:
                    Console.WriteLine(Season.Winter);
                    break;
                case 3:
                case 4:
                case 5:
                    Console.WriteLine(Season.Spring);
                    break;
                case 6:
                case 7:
                case 8:
                    Console.WriteLine(Season.Summer);
                    break;
                case 9:
                case 10:
                case 11:
                    Console.WriteLine(Season.Autumn);
                    break;
                default:
                    Console.WriteLine("There is no such month on this planet.");
                    break;
            }
        }
        static void Main(string[] args)
        {
            byte month = ReadMonth();
            DisplaySeason(month);
            Console.ReadKey();
        }
    }
}