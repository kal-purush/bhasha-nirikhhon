using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWorkHelper
{
    public class Helper
    {
        const string title = "Домашняя работа";
        const string fio = "Шаршуков Александр Сергеевич";
        const string MyLogin = "root";
        const string MyPassword = "GeekBrains";

        static bool fIsInitRecursiveSumm = false;
        static int ValueForRecursiveSumm;

        public static void PrintTitle()
        {
            Console.Title = title;
        }

        public static void PrintStudentInfo()
        {
            Console.WriteLine(fio);
        }

        /// <summary>
        /// Получить индекс массы тела (кг/м2)
        /// </summary>
        /// <param name="WeightInKg">Вес в килограммах</param>
        /// <param name="GrowthInCm">Рост в сантиметрах</param>
        /// <returns></returns>
        public static double GetBodyMassIndex(int WeightInKg, int GrowthInCm)
        {
            double dWeight = Convert.ToDouble(WeightInKg);
            double dGrowth = Convert.ToDouble(GrowthInCm) / 100;

            return dWeight / (dGrowth * dGrowth);
        }

        /// <summary>
        /// Получить диагноз по индексу массы тела
        /// </summary>
        /// <param name="BodyMassIndex">Индекс массы тела (кг/м2)</param>
        /// <returns></returns>
        public static string GetBodyMassIndexDiagnosis(double BodyMassIndex)
        {
            // Интерпретация индекса массы тела:
            // ИМТ < 18.5:	Ниже нормального веса
            // ИМТ >= 18.5 И < 25:	Нормальный вес
            // ИМТ >= 25 И < 30:	Избыточный вес
            // ИМТ >= 30 И < 35:	Ожирение I степени
            // ИМТ >= 35 И < 40:	Ожирение II степени
            // ИМТ >= 40:	Ожирение III степени

            string Diagnosis;

            if (BodyMassIndex < 18.5f)
            {
                Diagnosis = "Вес ниже нормального";
            }
            else if ((18.5f <= BodyMassIndex) && (BodyMassIndex < 25.0f))
            {
                Diagnosis = "Вес в норме";
            }
            else if ((25.0f <= BodyMassIndex) && (BodyMassIndex < 30.0f))
            {
                Diagnosis = "Избыточный вес";
            }
            else if ((30.0f <= BodyMassIndex) && (BodyMassIndex < 35.0f))
            {
                Diagnosis = "Ожирение I степени";
            }
            else if ((35.0f <= BodyMassIndex) && (BodyMassIndex < 40.0f))
            {
                Diagnosis = "Ожирение II степени";
            }
            else //if (BodyMassIndex >= 40.0f)
            {
                Diagnosis = "Ожирение III степени";
            }

            return Diagnosis;
        }

        /// <summary>
        /// Вычисление расстояния между двумя точками
        /// </summary>
        /// <param name="x1">Точка 1: координата X</param>
        /// <param name="y1">Точка 1: координата Y</param>
        /// <param name="x2">Точка 2: координата X</param>
        /// <param name="y2">Точка 2: координата Y</param>
        /// <returns></returns>
        public static double GetDistance(double x1, double y1, double x2, double y2)
        {
            return Math.Sqrt(Math.Pow(x2 - x1, 2) + Math.Pow(y2 - y1, 2));
        }

        /// <summary>
        /// Печать строки с заданными координатами
        /// </summary>
        /// <param name="str">Строка для печати</param>
        /// <param name="CursorX">Координаты курсора по горизонтали</param>
        /// <param name="CursorY">Координаты курсора по вертикали</param>
        public static void Print(string str, int CursorX, int CursorY)
        {
            Console.SetCursorPosition(CursorX, CursorY + 1);
            Console.WriteLine(str);
        }

        /// <summary>
        /// Проверка логина и пароля
        /// </summary>
        /// <param name="Login">Логин</param>
        /// <param name="Password">Пароль</param>
        /// <returns></returns>
        public static bool isValidAuthentication(string Login, string Password)
        {
            return ((MyLogin.Equals(Login)) && (MyPassword.Equals(Password))) ? true : false;
        }

        /// <summary>
        /// Получение минмального значения из трёх
        /// </summary>
        /// <param name="a">Значение 1</param>
        /// <param name="b">Значение 2</param>
        /// <param name="c">Значение 3</param>
        /// <returns></returns>
        public static int MIN(int a, int b, int c)
        {
            return a > b ? (c > b ? b : c) : (a < c ? a : c);
        }

        /// <summary>
        /// Проверка символа на число
        /// </summary>
        /// <param name="Ch">Символ для проверки</param>
        /// <returns></returns>
        public static bool IsNumber(char Ch)
        {
            return (('0' <= Ch) && (Ch <= '9')) ? true : false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Ch"></param>
        /// <returns></returns>
        public static bool IsNumberString(string Str)
        {
            for(int i = 0; i < Str.Length; i++)
            {
                if(!IsNumber(Str[i]))
                {
                    if (!((i == 0) && (Str[i] == '-')))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Проверка числа на нечетность
        /// </summary>
        /// <param name="Val"></param>
        /// <returns></returns>
        public static bool IsOdd(int Val)
        {
            return (Val % 2 > 0) ? true : false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name=""></param>
        /// <returns></returns>
        public static bool IsGoodNumber(int Number)
        {
            int Summ = GetSummDigitsOfNumber(Number);

            return (Number % Summ > 0) ? false : true;
        }

        /// <summary>
        /// Подсчет суммы чисел переданного числа
        /// </summary>
        /// <param name=""></param>
        public static int GetSummDigitsOfNumber(int Number)
        {
            string strNumber = Convert.ToString(Number);
            int Summ = 0;

            for(int i = 0; i < strNumber.Length; i++)
            {
                Summ += (strNumber[i] - '0');
            }

            return Summ;
        }

        /// <summary>
        /// Рекурсивный вывода чисел в диапазоне от Head до Tail
        /// </summary>
        /// <param name="Head">Начало диапазона</param>
        /// <param name="Tail">Конец диапазона</param>
        public static void PrintRange(int Head, int Tail)
        {
            Console.Write(Head + " ");

            if (Head == Tail)
            {
                return;
            }

            PrintRange(Head + 1, Tail);
        }

        /// <summary>
        /// Рекрсивный подсчет суммы чисел в диапазоне  от Head до Tail
        /// </summary>
        /// <param name="Head">Начало диапазона</param>
        /// <param name="Tail">Конец диапазона</param>
        /// <returns></returns>
        public static int GetSummInRange(int Head, int Tail)
        {
            if (!fIsInitRecursiveSumm)
            {
                fIsInitRecursiveSumm = true;
                ValueForRecursiveSumm = 0;
            }

            if (Head <= Tail)
            {
                ValueForRecursiveSumm += Head;
                Head = GetSummInRange(Head + 1, Tail);
            }

            fIsInitRecursiveSumm = false;
            return ValueForRecursiveSumm;
        }
    }
}