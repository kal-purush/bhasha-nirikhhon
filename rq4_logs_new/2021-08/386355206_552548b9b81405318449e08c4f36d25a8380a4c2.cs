using System;

// Частичные классы и методы.

// Частичные методы не могут иметь out параметры.

namespace Generics
{
    public partial class MyClass<T>
    {
        partial void PartialMethod<T>(T a, ref T b);
    }

    public partial class MyClass<T>
    {
        partial void PartialMethod<T>(T a, ref T b)
        {
            b = default(T);
            Console.WriteLine("{0}, {1}", a, b);
        }

        public void Proxy(T a, ref T b)
        {
            PartialMethod(a, ref b);
        }
    }

    class Program
    {
        static void Main()
        {
            MyClass<int> instance = new MyClass<int>();

            int operand1 = 1, operand2 = 2;

            instance.Proxy(operand1, ref operand2);

            Console.WriteLine(operand2);

            // Delay.
            Console.ReadKey();
        }
    }
}