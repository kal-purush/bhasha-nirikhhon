using System;

// Ковариантность массивов в C#.

namespace Covariant
{
    public interface IAnimal
    {
        void Voice();
    }

    public class Dog : IAnimal
    {
        public void Voice()
        {
            Console.WriteLine("Gav-Gav");
        }

        public void Jump()
        {
            Console.WriteLine("Jump");
        }
    }

    class Program
    {
        static void Main()
        {
            Dog[] dogs = { new Dog(), new Dog(), new Dog() };

            for (int i = 0; i < dogs.Length; i++)
            {
                dogs[i].Voice();
                dogs[i].Jump();
            }

            Console.WriteLine(new string('-', 10));

            IAnimal[] animal = dogs; // Ковариантность.

            for (int i = 0; i < dogs.Length; i++)
            {
                animal[i].Voice();
                //animal[i].Jump();
            }

            Console.WriteLine(new string('-', 10));

            dogs = (Dog[])animal;   // Не является Контрвариантностью.

            for (int i = 0; i < dogs.Length; i++)
            {
                dogs[i].Voice();
                dogs[i].Jump();
            }

            // Delay.
            Console.ReadKey();
        }
    }
}