using System;

namespace Lessons.General
{
    class Lesson1_Variables
    {
        public static void Start()
        {
            //////////////////////////////////////////////////////////////////////////1
            /// Существующие примитивные типы:

            // Целые типы
            sbyte   sbyteVal  = 0;   
            short   shortVal  = 0;  
            int     intVal    = 0;  
            long    longVal   = 0; 

            // Вещественные типы
            float   floatVal    = 123.123f; 
            double  doubleVal   = 123.123; 
            decimal decimalVal  = 123.123m; 

            // Символьный тип (можно задать символом в одинарных ковычках,
            // а можно числом(порядковый номер из таблицы ASCII))
            char charVal = 'a';
            charVal = (char)5;

            // Строчный тип
            string strVal = "Happy New Year 2019";  

            // Логический тип
            bool boolVal = true;


            // Для того что бы вывести значение какой либо переменной в консоль - 
            // можно воспользоваться Console.WriteLine(floatVal);
            // 
            // Например:
            Console.WriteLine("Пример того, как вывести в консоль значение переменной:");
            int x = 123;
            float y = 212.543f;
            Console.WriteLine("Переменная X = " + x);
            Console.WriteLine("Переменная Y = " + y);

            // Так-же можно использовать $ что бы сократить запись...
            // в таком случае - переменную нужно окружить фигурными кавычками
            //
            // Например:
            Console.WriteLine($"Переменная X = {x}");
            Console.WriteLine($"Переменная Y = {y}");
            Console.WriteLine("------------------\n");


            /////////////////////////////////////////////////////////////////////////2
            /// Диапазоны этих типов: 
            /// (имееется ввиду диапазон значений, которые можно поместить в этот тип)

            Console.WriteLine("Диапазоны значений примитивных типов:");
            Console.WriteLine($"sbyteVal -> MIN = {sbyte.MinValue},  MAX = {sbyte.MaxValue}");
            Console.WriteLine($"short    -> MIN = {short.MinValue}, MAX = {short.MaxValue}");
            Console.WriteLine($"int      -> MIN = {int.MinValue}, MAX = {int.MaxValue}");
            Console.WriteLine($"long     -> MIN = {long.MinValue}, MAX = {long.MaxValue}");
            Console.WriteLine();

            Console.WriteLine($"float     -> MIN = {float.MinValue}, MAX = {float.MaxValue}");
            Console.WriteLine($"double    -> MIN = {double.MinValue}, MAX = {double.MaxValue}");
            Console.WriteLine($"decimal   -> MIN = {decimal.MinValue}, MAX = {decimal.MaxValue}");
            Console.WriteLine();

            Console.WriteLine($"Char      -> MIN = {Convert.ToInt32(char.MinValue)}, MAX = {Convert.ToInt32(char.MaxValue)}");
        }
    }
}