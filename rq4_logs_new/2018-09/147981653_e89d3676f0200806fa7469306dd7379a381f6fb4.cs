using System;

namespace Project1
{
    class Shape
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter Width: ");
            string userInputWidth = Console.ReadLine(); 
            Console.WriteLine("Enter Length: ");
            string userInputLength = Console.ReadLine();
            int width = Convert.ToInt32(userInputWidth);
            int length = Convert.ToInt32(userInputLength);
            int perimeter = width + width + length + length;
            Console.WriteLine();
            Console.WriteLine("The perimeter is {0}", perimeter);
        }
    }
}