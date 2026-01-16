using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Homework1_2_9
{
    /// <summary>
    /// 2.9. Сделать программу перевода валют. Вводится сумма, потом выбирается вид валюты – гривны или доллары или евро,
    /// программа переводит в две другие валюты. Текущий курс валюты считать известным.
    /// </summary>
    class Program
    {        
        /// <summary>
        /// возможные валюты
        /// </summary>
        enum Currency { GRN, USD, EUR };

        /// <summary>
        /// массив коэффециентов перевода валют
        /// </summary>
        private static double[][] priceCurrency = new double[][] {
                        new double[]{ 1 , 0.0385 , 0.0312 },
                        new double[]{ 25.9864 , 1 , 0.8115 },
                        new double[]{ 32.023 , 1.2323 , 1 } };

        /// <summary>
        /// запрашиваем у пользователя сумму
        /// </summary>
        /// <returns>int сумма наличных</returns>
        static int GetCash()
        {
            string enter;
            while (true)
            {
                Console.Write("Введите сумму, которая у вас есть на руках: ");
                enter = Console.ReadLine();
                if (int.TryParse(enter, out int number))
                {
                    if (number > 0)
                    {
                        return int.Parse(enter);
                    }
                }
            };
        }

        /// <summary>
        /// запрашиваем у пользователя валюту
        /// </summary>
        /// <returns>Currency - валюта</returns>
        static Currency GetCurrency()
        {
            string enter;
            while (true)
            {
                Console.WriteLine("0 - " + Currency.GRN + "\n1 - " + Currency.USD + "\n2 - " + Currency.EUR);
                Console.Write("Выбирите валюту: ");
                enter = Console.ReadLine();
                if (int.TryParse(enter, out int number))
                {
                    if (number >= (int)Currency.GRN || number <= (int)Currency.EUR)
                    {
                        return (Currency)int.Parse(enter);
                    }
                }
            };
        }

        /// <summary>
        /// выводим результат
        /// </summary>
        /// <param name="cash">сумма наличных</param>
        /// <param name="currency">валюта</param>
        static void ShowResult(int cash, Currency currency)
        {
            for (int i = 0; i < priceCurrency[(int)currency].Length; i++)
            {
                if (i != (int)currency)
                {
                    int round = 2;
                    Console.WriteLine(Enum.GetName(typeof(Currency), i) + "" +
                        " : " + Math.Round(cash * priceCurrency[(int)currency][i], round));
                }
            }
        }

        static void Main(string[] args)
        {
            //наличнве
            int cash = GetCash();

            //валюта
            Currency currency = GetCurrency();

            //показываем результат перевода в другую валюту
            ShowResult(cash, currency);
        }
    }
}