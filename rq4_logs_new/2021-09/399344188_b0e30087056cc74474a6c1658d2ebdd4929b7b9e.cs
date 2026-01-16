using System;
using System.IO;

namespace Project3
{
    class Program
    {
        static void Main(string[] args)
        {
            byte[] arr = new byte[1000];
            string tfile = "binfile.bin";
            Console.Write("Введите количество чисел: ");
            int N = Convert.ToInt32(Console.ReadLine());
            for (int i = 0; i < N; i++)
            {
                byte x = Convert.ToByte(Console.ReadLine());
                arr[i] = x;
                
            }
            {
                
                File.WriteAllBytes(tfile, arr);
            }
        }
    }
}