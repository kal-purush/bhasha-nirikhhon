using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConstructsApp
{
    public class FunWithArray
    {

        public static void ArrayInitialization()
        {
            Console.WriteLine("=> Array Initialization.");

            // инициализация с использованием ключевого слова new
            string[] stringArray = new string[] { "one", "two", "three" };
            Console.WriteLine("stringArray has {0} elements", stringArray.Length);
            // без new
            bool[] boolArray = { false, false, true };
            Console.WriteLine("boolArray has {0} elements", boolArray.Length);
            // с указанием размера
            int[] intArray = new int[4] { 20, 22, 23, 0 };
            Console.WriteLine("intArray has {0} elements", intArray.Length);


            //Неявно типизированные

            // a is really int[].
            var a = new[] { 1, 10, 100, 1000 };
            Console.WriteLine("a is a: {0}", a.ToString());
            // b is really double[].
            var b = new[] { 1, 1.5, 2, 2.5 };
            Console.WriteLine("b is a: {0}", b.ToString());
            // c is really string[].
            var c = new[] { "hello", null, "world" };
            Console.WriteLine("c is a: {0}", c.ToString());
            

            // А тут будет ошибка из-зи смешанных типов
            //var d = new[] { 1, "one", 2, "two", false };
            // Тут ошибка, потому что не на что ориентироваться и тип опреден быть не может
            //var d = new[] { };



            Console.ReadLine();

        }
    }
}