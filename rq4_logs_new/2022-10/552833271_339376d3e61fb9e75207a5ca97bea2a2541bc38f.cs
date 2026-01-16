using System;

namespace CalculatorV2
{
    class Calculator
    {
        public Calculator(int a, int b)
        {

        }

        public int Add(int a, int b)
        {
            return a + b;
        }

        public int Subtract(int a, int b)
        {
            return a - b;
        }

        public int Multiply(int a, int b)
        {
            return a * b;
        }

        public int Divide(int a, int b)
        {
            try
            {
                int c = a / b;
            }
            catch(DivideByZeroException ex)
            {
                Console.WriteLine("Error");
            }

            return 0;
            
        }

        public int Square(int a)
        {
            return Convert.ToInt32(Math.Pow(a, 2));
        }

        public int Squareroot(int a)
        {
            return Convert.ToInt32(Math.Sqrt(a));
        }

        public int Cube(int a)
        {
            return Convert.ToInt32(Math.Pow(a, 3));
        }

        public int Cuberoot(int a)
        {
            return Convert.ToInt32(Math.Cbrt(a));
        }

        public int Log10(int a)
        {
            return Convert.ToInt32(Math.Log10(a));
        }
    }
    
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Calculator");
            Console.WriteLine("-------------------------------");
            Console.WriteLine("1.Add");
            Console.WriteLine("2.Subtract");
            Console.WriteLine("3.Multiply");
            Console.WriteLine("4.Divide");
            Console.WriteLine("5.Square");
            Console.WriteLine("6.Squareroot");
            Console.WriteLine("7.Cube");
            Console.WriteLine("8.Cuberoot");
            Console.WriteLine("9.log10");
            Console.WriteLine("Enter Choice(1-9):");
            int a, b, c;
            

            int ch = Int32.Parse(Console.ReadLine());
            switch (ch)
            {
                case 1:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    Console.Write("Enter b:");
                    b = Convert.ToInt32(Console.ReadLine());

                    Calculator Calc = new Calculator(a, b);
                    c = Calc.Add(a, b);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 2:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    Console.Write("Enter b:");
                    b = Convert.ToInt32(Console.ReadLine());

                    Calculator Calc2 = new Calculator(a, b);
                    c = Calc2.Subtract(a, b);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 3:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    Console.Write("Enter b:");
                    b = Convert.ToInt32(Console.ReadLine());

                    Calculator Calc3 = new Calculator(a, b);
                    c = Calc3.Multiply(a, b);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 4:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    Console.Write("Enter b:");
                    b = Convert.ToInt32(Console.ReadLine());

                    Calculator Calc4 = new Calculator(a, b);
                    c = Calc4.Divide(a, b);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 5:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    b = 0;
                    Calculator Calc5 = new Calculator(a, b);
                    c = Calc5.Square(a);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 6:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    b = 0; 

                    Calculator Calc6 = new Calculator(a, b);
                    c = Calc6.Squareroot(a);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 7:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    b = 0;

                    Calculator Calc7 = new Calculator(a, b);
                    c = Calc7.Cube(a);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 8:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    b = 0;

                    Calculator Calc8 = new Calculator(a, b);
                    c = Calc8.Cuberoot(a);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                case 9:
                    Console.Write("Enter a:");
                    a = Convert.ToInt32(Console.ReadLine());
                    b = 0;

                    Calculator Calc9 = new Calculator(a, b);
                    c = Calc9.Log10(a);
                    Console.WriteLine("Answer = {0}", c);
                    break;
                default:
                    Console.WriteLine("Invalid Choice");
                    break;

            }
            Console.ReadKey();
        }
    }


}