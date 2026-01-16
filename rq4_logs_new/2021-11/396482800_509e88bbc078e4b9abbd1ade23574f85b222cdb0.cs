using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeworkSerg.General
{
    class Homework2_Convert
    {
        public static void Start()
        {
            //Черновик
            Console.WriteLine($"int min = {int.MinValue}, max = {int.MaxValue}");
            Console.WriteLine();
            Console.WriteLine($"Char -> MIN = {Convert.ToInt32(char.MinValue)}, MAX = {Convert.ToInt32(char.MaxValue)}");
            Console.WriteLine();
            Console.WriteLine($"long     -> MIN = {long.MinValue}, MAX = {long.MaxValue}");
            Console.WriteLine();

            //Явное преобр. (Из большего в меньшее)
            long lngVl = 9223372036854775807;
            Console.WriteLine($"long1 = {lngVl}");
            Console.WriteLine();
            int intVl = (int)lngVl;
            Console.WriteLine($"long1 > int = {intVl}");
            Console.WriteLine();
            short shlVl = (short)lngVl;
            Console.WriteLine($"long1 > short = {shlVl}");
            Console.WriteLine();

            long lngVl2 = 922337203685477580;
            Console.WriteLine($"long2 = {lngVl2}");
            Console.WriteLine();
            int intVl2 = (int)lngVl2;
            Console.WriteLine($"long2 > int = {intVl2}");
            Console.WriteLine();
            short shlVl2 = (short)lngVl2;
            Console.WriteLine($"long2 > short = {shlVl2}");
            Console.WriteLine();

            long lngVl3 = 92233720368547758;
            Console.WriteLine($"long3 = {lngVl3}");
            Console.WriteLine();
            int intVl3 = (int)lngVl3;
            Console.WriteLine($"long3 > int = {intVl3}");
            Console.WriteLine();
            short shlVl3 = (short)lngVl3;
            Console.WriteLine($"long3 > short = {shlVl3}");
            Console.WriteLine();
            //Почему число отличающееся на "10", "100" или "1000" сначала -1 (ошибка, потом минусовое потом целое ?

            //Десетичные
            double doubleVal = 987.654321;
            Console.WriteLine(doubleVal);
            Console.WriteLine();
            int intVal = (int)doubleVal;
            Console.WriteLine(intVal);
            Console.WriteLine();
            float floatVal = (float)doubleVal;
            Console.WriteLine(floatVal);
            Console.WriteLine();

            long iiVAL = 123123123;
            Console.WriteLine(iiVAL);
            float fval = iiVAL;
            Console.WriteLine(fval);
            Console.WriteLine();

            int inVl2 = 98756484;
            sbyte shVl2 = (sbyte)inVl2;
            Console.WriteLine(inVl2);
            Console.WriteLine(shVl2);
            Console.WriteLine();
            int x = 123;
            float y = 212.543f;
            Console.WriteLine("Переменная X = " + x);
            Console.WriteLine("Переменная Y = " + y);
            Console.WriteLine();

            //Пример
            //string strValue1 = Console.ReadLine();
            //string strValue2 = Console.ReadLine();

            //Console.WriteLine($"val1 = {strValue1}, val2 = {strValue2}");
            //Console.WriteLine($"str: str + str -> {strValue1 + strValue2}");

            //int val1 = Convert.ToInt32(strValue1);
            //int val2 = Convert.ToInt32(strValue2);
            //Console.WriteLine($"int: int + int -> {val1 + val2}");
            //Console.WriteLine();
            //Как закомитеть много строк за один раз ?

            //Мое
            string namb1 = Console.ReadLine();
            string namb2 = Console.ReadLine();

            Console.WriteLine($"namb1 = {namb1}, namb2 = {namb2}");
            Console.WriteLine($"namb ov = namb1 + namb2 =  {namb1 + namb2}");

            int namb3 = Convert.ToInt32(namb1);
            int namb4 = Convert.ToInt32(namb2);
            Console.WriteLine($"namb ov = namb1 + namb2 =  {namb3 + namb4}");
            Console.WriteLine();

            int namb5 = 1000;
            int namb6 = 1000;
            Console.WriteLine($"namb5 = {namb5}");
            Console.WriteLine($"namb6 = {namb6}");
            Console.WriteLine($"namb5 + namb6 = {namb5 + namb6}");
            string str1 = Convert.ToString(namb5);
            string str2 = Convert.ToString(namb6);
            Console.WriteLine($"namb5 + namb6 = {str1 + str2}");




        }
    }
}