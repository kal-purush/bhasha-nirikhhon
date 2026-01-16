using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Lesson_16
{
    class Program
    {
        static void Main(string[] args)
        {
            //// Объявление строк
            //string s1 = "Hello, world!";
            //string s2 = null;
            //string s3 = new string('a', 6);
            //string s4 = new string(new char[] { 'a', 'b', 'c' });
            //string s5 = new string(new char[] { 'a', 'a', 'a', 'a', 'a', 'a' });
            //string s6 = string.Empty;
            //string s6_2 = "";

            //// Сравнение строк
            //Console.WriteLine(s1 == "Hello world!");
            //Console.WriteLine(s1 == s2);
            //Console.WriteLine(s3 == s5);
            //Console.WriteLine(s3.Equals(s5));

            //// Свойства строк
            //Console.WriteLine(s1);
            //Console.WriteLine("Length: " + s1.Length);

            //// Индексатор
            //for (int i = 0; i < s1.Length; i++)
            //{
            //    Console.Write(s1[i] + " ");
            //}

            //Console.WriteLine();

            //// Управляющие последовательности символов

            //Console.WriteLine("ААААА\tAAAAA");
            //Console.WriteLine("ААААА\fAAAAA");
            //Console.WriteLine("ААААА\nAAAAA");
            //Console.WriteLine("ААААА\vAAAAA");
            //Console.WriteLine("ААААА\bAAAAA");
            //Console.WriteLine("ААААА\rAAAAA");

            ////string[] s = { "\\", ":", "/", "-" };
            ////for (int i = 0; i < 10000; i++)
            ////{
            ////    Console.Write(s[i % s.Length] + '\r');
            ////    Thread.Sleep(400);
            ////}

            //// Стандартные и дословные литералы

            //string fileName1 = "D:\\folder\\file.txt";
            //string fileName2 = @"D:\folder\file.txt";
            //Console.WriteLine();
            //Console.WriteLine("Стандартный литерал: " + fileName1);
            //Console.WriteLine("Дословный (экранирование)" + fileName2);
            //Console.WriteLine(fileName1 == fileName2);
            //Console.WriteLine();

            //// Осторожно, трюк выполнен профессионалами! Не повторять без подготовки!
            //Console.WriteLine();
            ////ReverseInternedString();
            //Console.WriteLine();

            //// Методы класса String
            //Console.WriteLine();
            //Console.WriteLine(s1.Substring(7, 4));
            //Console.WriteLine(s1);
            //var s7 = s1.Substring(7);

            //Console.WriteLine();
            //Console.WriteLine(s1.Replace("l", "L"));
            //Console.WriteLine(s1);

            //Console.WriteLine();
            //Console.WriteLine(s1.Remove(7, 3));
            //Console.WriteLine(s1);

            //Console.WriteLine();
            //Console.WriteLine(s1.Insert(7, "HALLELUJAH"));
            //Console.WriteLine(s1);

            //Console.WriteLine();
            //Console.WriteLine(string.Concat("AAAA", "BBBB"));
            //Console.WriteLine("AAAA" + "BBBB");

            //Console.WriteLine();
            //Console.WriteLine(s1.StartsWith("Hell"));
            //Console.WriteLine(s1.EndsWith("!"));
            //Console.WriteLine(s1.IndexOf('o'));
            //Console.WriteLine(s1.Contains("World"));
            //Console.WriteLine(s1.Contains("world"));


            //Console.WriteLine();
            //var ip = string.Join(".", 192, 168, 0, 1);
            //Console.WriteLine(ip);
            //var split = ip.Split('.');
            //for (int i = 0; i < split.Length; i++)
            //{
            //    Console.WriteLine(split[i]);
            //}
            //Console.WriteLine();

            //// Форматирование строк
            //string formattedString = string.Format("{0}", "argument");
            //Console.WriteLine(formattedString);
            //Console.WriteLine("{0}", "test argument for formatting");
            //Console.WriteLine("{0} + {1} = {2}", 5, 7, 12);
            //Console.WriteLine(5 + " + " + 7 + " = " + 12);
            //Console.WriteLine($"{5} + {7} = {12}");

            //Console.WriteLine("{0}", 835.12345678);
            //Console.WriteLine("{0,10}", 835.12345678);
            //Console.WriteLine("{0,15}", 835.12345678);
            //Console.WriteLine("{0,-20}", 835.12345678);

            //Console.WriteLine();
            
            //Console.WriteLine("{0:E}", 835.12345678);
            //Console.WriteLine("{0:F}", 835.12345678);
            //Console.WriteLine("{0:N}", 835.12345678);
            //Console.WriteLine("{0:P}", 835.12345678);

            //Console.WriteLine();

            //Console.WriteLine(DateTime.Now.ToString());
            //Console.WriteLine(DateTime.Now.ToString("yyyy"));
            //Console.WriteLine(DateTime.Now.ToString("dd/yyyy"));
            //Console.WriteLine(DateTime.Now.ToString("mm/dd/yyyy"));
            //Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy"));
            //Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"));
            //Console.WriteLine(DateTime.Now.ToString("HH"));
            //Console.WriteLine(DateTime.Now.ToString("HH:mm"));
            //Console.WriteLine(DateTime.Now.ToString("HH:mm:ss"));

            //Console.WriteLine();

            //ConcatString();
            //Console.Read();
            ConcatStringBuilder();
            Console.Read();
            ConcatStringBuilderV2();

            Console.Read();
            Console.Read();
        }

        static unsafe void ReverseInternedString()
        {
            var s = "Strings are immutable";
            Console.WriteLine(s);
            int length = s.Length;
            unsafe
            {
                fixed (char* c = s)
                {
                    for (int i = 0; i < length / 2; i++)
                    {
                        var temp = c[i];
                        c[i] = c[length - i - 1];
                        c[length - i - 1] = temp;
                    }
                }
            }
            Console.WriteLine("Strings are immutable");
        }
        
        static void ConcatString()
        {
            Console.WriteLine("String");
            Stopwatch sw = new Stopwatch();

            string s = string.Empty;
            sw.Start();
            for (int i = 0; i < 100; i++)
            {
                s += i;
            }
            sw.Stop();
            Console.WriteLine("Concat string: 100, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            s = string.Empty;
            sw.Start();
            for (int i = 0; i < 10000; i++)
            {
                s += i;
            }
            sw.Stop();
            Console.WriteLine("Concat string: 10000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            s = string.Empty;
            sw.Start();
            for (int i = 0; i < 100000; i++)
            {
                s += i;
            }
            sw.Stop();
            Console.WriteLine("Concat string: 100000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();
            
            Console.WriteLine(s);
            Console.WriteLine();
        }

        static void ConcatStringBuilder()
        {
            Console.WriteLine("StringBuilder");
            Stopwatch sw = new Stopwatch();

            StringBuilder sb = new StringBuilder();
            sw.Start();
            for (int i = 0; i < 100; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 100, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sb.Clear();
            sw.Start();
            for (int i = 0; i < 10000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 10000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sb.Clear();
            sw.Start();
            for (int i = 0; i < 100000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 100000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sb.Clear();
            sw.Start();
            for (int i = 0; i < 2000000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 2000000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sb.Clear();
            sw.Start();
            for (int i = 0; i < 20000000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 20000000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();
            //Console.WriteLine(sb.ToString());
        }

        static void ConcatStringBuilderV2()
        {
            Console.WriteLine("StringBuilder v2");
            Stopwatch sw = new Stopwatch();

            StringBuilder sb = new StringBuilder(25000000);
            sw.Start();
            for (int i = 0; i < 100; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 100, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sw.Start();
            for (int i = 0; i < 10000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 10000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sb.Clear();
            sw.Start();
            for (int i = 0; i < 100000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 100000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sb.Clear();
            sw.Start();
            for (int i = 0; i < 2000000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 2000000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();

            sb.Clear();
            sw.Start();
            for (int i = 0; i < 20000000; i++)
            {
                sb.Append(i);
            }
            sw.Stop();
            Console.WriteLine("Concat StringBuilder: 20000000, Elapsed ms: " + sw.ElapsedMilliseconds);
            sw.Reset();
            //Console.WriteLine(sb.ToString());
        }
    }
}