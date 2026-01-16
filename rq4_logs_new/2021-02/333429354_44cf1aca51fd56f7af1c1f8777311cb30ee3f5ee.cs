using System;
using System.Threading;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TMS.Net07.Homework.ConverterCurrency
{
    class Program
    {
        static void Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine("Input exit if you want to close the program\n\n");
                Console.WriteLine("Input source currency: ");
                var sourcecurrency = Console.ReadLine().ToUpper();
                if (sourcecurrency == "EXIT")
                {
                    Console.WriteLine("You closed the program!");
                    return;
                }
                if (!(sourcecurrency == "USD" || sourcecurrency == "RUB" || sourcecurrency == "EUR" || sourcecurrency == "BYN"))
                {
                    Console.WriteLine("bad input!");
                    return;
                }

                Console.WriteLine("Input target currency: ");
                var targetcurrency = Console.ReadLine().ToUpper();
                if (!(targetcurrency == "USD" || targetcurrency == "RUB" || targetcurrency == "EUR" || targetcurrency == "BYN"))
                {
                    Console.WriteLine("bad input!");
                    return;
                }

                Console.WriteLine("Input amount: ");
                string amount = Console.ReadLine();
                //сделал так, чтобы ввод можно было делать через точку. без этого не работает
                CultureInfo temp_culture = Thread.CurrentThread.CurrentCulture;
                Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
                decimal mainamount = Convert.ToDecimal(amount);
                Thread.CurrentThread.CurrentCulture = temp_culture;

                decimal solution = 0;

                if (sourcecurrency == "USD")
                {
                    if (targetcurrency == "RUB")
                    {
                        solution = mainamount * 75.5051m;
                    }
                    else if (targetcurrency == "EUR")
                    {
                        solution = mainamount * 0.8252m;
                    }
                    else if (targetcurrency == "USD")
                    {
                        solution = mainamount * 1.0000m;
                    }
                    else if (targetcurrency == "BYN")
                    {
                        solution = mainamount * 2.6193m;
                    }
                }
                else if (sourcecurrency == "BYN")
                {
                    if (targetcurrency == "EUR")
                    {
                        solution = mainamount * 0.3150m;
                    }
                    else if (targetcurrency == "BYN")
                    {
                        solution = mainamount * 1.0000m;
                    }
                    else if (targetcurrency == "RUB")
                    {
                        solution = mainamount * 28.8243m;
                    }
                    else if (targetcurrency == "USD")
                    {
                        solution = mainamount * 0.3818m;
                    }
                }
                else if (sourcecurrency == "RUB")
                {
                    if (targetcurrency == "EUR")
                    {
                        solution = mainamount * 0.0109m;
                    }
                    else if (targetcurrency == "BYN")
                    {
                        solution = mainamount * 0.0347m;
                    }
                    else if (targetcurrency == "RUB")
                    {
                        solution = mainamount * 1.0000m;
                    }
                    else if (targetcurrency == "USD")
                    {
                        solution = mainamount * 0.0132m;
                    }
                }
                else if (sourcecurrency == "EUR")
                {
                    if (targetcurrency == "EUR")
                    {
                        solution = mainamount * 1.0000m;
                    }
                    else if (targetcurrency == "BYN")
                    {
                        solution = mainamount * 3.1742m;
                    }
                    else if (targetcurrency == "RUB")
                    {
                        solution = mainamount * 91.4940m;
                    }
                    else if (targetcurrency == "USD")
                    {
                        solution = mainamount * 1.2118m;
                    }
                }
                Console.WriteLine($"{amount} {sourcecurrency} is equal to {solution}  {targetcurrency}\n\n");
                Console.ReadLine();
                Console.Clear();
            }                 
        }       
    }
}
