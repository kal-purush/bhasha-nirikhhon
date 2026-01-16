using System;

// Ковариантность не применима к массивам элементов структурных типов.

namespace ArrayCovariant
{
    public interface IAnimal
    {
        void Voice();
    }

    public struct Dog : IAnimal
    {
        public void Voice()
        {
            Console.WriteLine("Gav-Gav");
        }
    }

    class Program
    {
        static void Main()
        {
            // 1
            Dog[] dogs = { new Dog(), new Dog(), new Dog() };

            //IAnimal[] animal = dogs; // Ковариантность.
            //dogs = array;  // Контрвариантность.
            ValueType i = new Int32() as ValueType;

            // 2
            int[] vector = new int[3] { 1, 2, 3 };
            //object[] array = vector; // Ковариантность

            // Противоречие между системой типов CLI и системой типов C#.
            // (ковариантность массивов типов-значений несогласована)
            
            uint[] array = new uint[3];

            Console.WriteLine("array  {0} {1}", array is uint[], array is int[]);

            object @object = array;

            Console.WriteLine("object {0} {1}", @object is uint[], @object is int[]);

            // Delay.
            Console.ReadKey();
            // Задержка.
            Console.ReadKey();
        }
    }
}