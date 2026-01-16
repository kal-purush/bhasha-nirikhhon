using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConstructsApp
{
    class Params
        
    {

        public static void ParametersModifiers()
        {
            int x = 10;
            Console.WriteLine("None");
            Console.WriteLine($"Наш x до вызова ф-ии без модификатора параметра: {x}");
            int ResNoneModifier = ParamsNoneModifier(x);
            Console.WriteLine($"Наш x ПОСЛЕ вызова ф-ии без модификатора: {x}");
            Console.WriteLine($"Результат ф-ии (x*2): {ResNoneModifier}");
            Console.WriteLine($"БЕЗ модификатора - идет обработка копии икса (Относится к value type)");

            Console.WriteLine("------------------------------------------------------------------------");
            Console.WriteLine("OUT");
            Console.WriteLine($"Наш x до вызова ф-ии OUT параметра: {x}");
            ParamsOutModifier(out x);
            Console.WriteLine($"Наш x ПОСЛЕ вызова ф-ии OUT: {x}");
            Console.WriteLine($"Нуждается в явном определении  внутри ф-ии, но вовне может быть неопределен");
            int y;
            Console.WriteLine($"Наш y ( int y;) до вызова ф-ии OUT параметра НЕ определен");
            ParamsOutModifier(out y);
            Console.WriteLine($"Наш x ПОСЛЕ вызова ф-ии OUT: {y}");

            Console.WriteLine("------------------------------------------------------------------------");
            Console.WriteLine("REF");
            int z = 10;
            Console.WriteLine($"Наш z до вызова ф-ии REF параметра: {z}");
            ParamsRefModifier(ref z);
            Console.WriteLine($"Наш z ПОСЛЕ вызова ф-ии REF (x*2): {z}");
            Console.WriteLine($"Нуждается в явном определении во вне, в самой ф-ии может не определяться");
            Console.WriteLine("------------------------------------------------------------------------");
            Console.WriteLine("Params int[]");
            int[] arr = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            Console.WriteLine($"Массив до вызова ф-ии: {string.Join(",",arr)}");
            Console.WriteLine($"Результат сцммирования: {ParamsParamsModifier(arr)}");
            Console.WriteLine("------------------------------------------------------------------------");
            Console.WriteLine($"OPTIONAL args\nВвожу только имя для ф-ии OptionalParam(string Name, string Prof = 'Programmer'): ");
            OptionalParam(Name: "Sasha");
            Console.WriteLine($"Ввожу все аргументы ф-ии OptionalParam(string Name, string Prof = 'Programmer'): ");
            OptionalParam(Name: "Sasha",Prof: "Illustrator");


            Console.ReadLine();
        }

        static int ParamsNoneModifier(int x)
        {
            return x * 2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"> out param.  x = x*2 DON'T WORK</param>
        static void ParamsOutModifier(out int x)
        {
            //x = x*2;
            x = 20;
        }

        static void ParamsRefModifier(ref int x)
        {
            x = x * 2;
        }

        static int ParamsParamsModifier(params int[] i)
        {
            return i.Sum();
        }

        static void OptionalParam(string Name, string Prof = "Programmer")
        {
            Console.WriteLine($"Name = {Name}, Prof = {Prof}");
        }

    }
}