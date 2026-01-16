using System;

namespace Delegates
{
    //1
    public delegate double ArithmeticOperation(double a, double b);
    //2
    public delegate double AverageFromDelegates(RandomValue[] array);
    public delegate int RandomValue();
    //3
    public delegate double Average(int a, int b, int c);

    class Program
    {
        static void Main(string[] args)
        {
            //1
            ArithmeticOperation Add = (a, b) => { return a + b; };
            Console.WriteLine(Add(15, 30));

            ArithmeticOperation Sub = (a, b) => { return a - b; };
            Console.WriteLine(Sub(15, 30));

            ArithmeticOperation Mul = (a, b) => { return a * b; };
            Console.WriteLine(Mul(15, 30));

            ArithmeticOperation Div = (a, b) => { return b != 0 ? a / b : Double.NaN; };
            Console.WriteLine(Div(15, 30));

            Console.WriteLine();
            //2
            RandomValue randomValue = () => { return new Random().Next(10); };

            AverageFromDelegates averageFromDelegates = (RandomValue[] delegatesArray) =>
            {
                double sum = 0;
                foreach (RandomValue value in delegatesArray)
                {
                    sum += value();
                }
                return sum / delegatesArray.Length;
            };

            Console.WriteLine(averageFromDelegates(new[] { randomValue, randomValue, randomValue, randomValue }));

            Console.WriteLine();
            //3
            Average average = (int a, int b, int c) => { return (a + b + c) / 3.0; };
            Console.WriteLine(average(10, 15, 7));

            Console.WriteLine();
            //4
            SumFromOneToThousand((int sum) => { Console.WriteLine(sum); });
            SumFromOneToThousand(OutConsole);

            Console.ReadLine();
        }

        public static void SumFromOneToThousand(Action<int> outputCallback)
        {
            int sum = 0;
            for (int i = 1; i <= 1000; i++)
            {
                sum += i;
            }
            outputCallback(sum);
        }

        public static void OutConsole(int sum)
        {
            Console.WriteLine(sum);
        }
    }
}