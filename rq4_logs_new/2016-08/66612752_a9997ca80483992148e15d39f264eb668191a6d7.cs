using System;

namespace SimpleArraySum
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var read = Console.ReadLine();
            int n = 0;
            if (!int.TryParse(read, out n)) return;
            read = Console.ReadLine();
            var arr = read.Split(' ');
            var sum = 0;
            int parser = 0;
            for(var i = 0; i < arr.Length; i++)
            {
                if (!int.TryParse(arr[i], out parser)) return;
                sum += parser;
            }
            Console.WriteLine(sum);
        }
    }
}