using System;

namespace PrintDayInWord
{
    class Program
    {
        static void Main(string[] args)
        {
            int dayNumber = 1;
            if (dayNumber == 0)
                Console.WriteLine($" {dayNumber} stands for Sunday");
            else if (dayNumber == 1)
                Console.WriteLine($" {dayNumber} stands for Monday");
            else if (dayNumber == 2)
                Console.WriteLine($" {dayNumber} stands for Tuesday");
            else if (dayNumber == 3)
                Console.WriteLine($" {dayNumber} stands for Wednesday");
            else if (dayNumber == 4)
                Console.WriteLine($" {dayNumber} stands for Thursday");
            else if (dayNumber == 5)
                Console.WriteLine($" {dayNumber} stands for Friday");
            else if (dayNumber == 6)
                Console.WriteLine($" {dayNumber} stands for Saturday");
            else Console.WriteLine($" {dayNumber} is not a valid day");
            
            switch (dayNumber)
            {
                case 0: 
                    Console.WriteLine($" {dayNumber} stands for Sunday");
                    break;
                case 1: 
                    Console.WriteLine($" {dayNumber} stands for Monday");
                    break;
                case 2: 
                    Console.WriteLine($" {dayNumber} stands for Tuesday");
                    break;
                case 3: 
                    Console.WriteLine($" {dayNumber} stands for Wednesday");
                    break;
                case 4: 
                    Console.WriteLine($" {dayNumber} stands for Thursday");
                    break;
                case 5: 
                    Console.WriteLine($" {dayNumber} stands for Friday");
                    break;
                case 6: 
                    Console.WriteLine($" {dayNumber} stands for Saturday");
                    break;
                default: 
                    Console.WriteLine($" {dayNumber} is not a valid day");
                    break;
            }
            
                Console.ReadKey();
        }
    }
}