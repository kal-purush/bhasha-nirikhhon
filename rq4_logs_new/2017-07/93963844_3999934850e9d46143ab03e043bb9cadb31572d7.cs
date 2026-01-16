using System;

namespace ConsoleApplication1
{
    class Calculator
    {
        static void Main()
        {
            int num = 0; // local variables
            Console.WriteLine("num: " + num);

            Number obj1 = new Number();
            Console.WriteLine("obj1.numbrX: " + obj1.numberX);
            Console.WriteLine("obj1.numbrY: " + obj1.numberY);

            Number obj2 = new Number(100);
            Console.WriteLine("obj2.numbrX: " + obj2.numberX);
            Console.WriteLine("obj2.numbrY: " + obj2.numberY);

            Number obj = new Number(10, 20); // creating object of NUmber class
            Console.WriteLine("obj.numbrX: " + obj.numberX);
            Console.WriteLine("obj.numbrY: " + obj.numberY);


            Console.ReadLine();

        }
    }
    class Number
    {
        // class members or data members or fields or attributes
        public int numberX;  // declaration
        public int numberY;  // declaration

        //Constructor with zero arguments
        public Number()
        {
            Console.WriteLine("\nDefault constructor - Number()");
            numberX = 1; // initialization
            numberY = 1; // initialization
        }
        // Constructor with one integer parameter
        public Number(int n)
        {
            Console.WriteLine("\nParameterrized constructor - Number(int n)");
            numberX = n;
            numberY = n;
        }

        // Constructor with two integer parameters
        public Number(int x, int y)
        {
            Console.WriteLine("\nParameterrized constructor - Number(int x, int y)");
            numberX = x;
            numberY = y;
        }
    }
}