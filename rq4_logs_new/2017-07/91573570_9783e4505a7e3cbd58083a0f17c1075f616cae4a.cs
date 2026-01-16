using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task4
{
    class Program
    {
        static void Main(string[] args)
        {
            string input = Console.ReadLine();
            string filename = "test" + input + ".txt";
            if (!File.Exists(filename))
            {
                Console.WriteLine("Файл не существует");
                return;
            }
            StreamReader reader = new StreamReader(filename);
            if (reader.EndOfStream)
            {
                Console.WriteLine("Файл пуст");
                return;
            }
            int C = 0;
            int L = 0;
            int i = 0;
            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                string[] mass = line.Split(' ');
                L = mass.Length;
                i = 0;
                while (i < L)
                {
                    if (!String.IsNullOrWhiteSpace(mass[i]))  
                    {
                        C++;
                    }
                    i++;
                }
            }
            reader.Close();
            Console.Write(C);
            }
        }
    }
