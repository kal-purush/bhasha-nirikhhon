using System;

namespace ReverseBinary
{
    class Program
    {
        public static string ReverseBinary(int userNumbers)
        {
            
            int[] binary = new int[4];
            
            for (int i = 0; userNumbers > 0; i++)
            {
                 binary[i] = userNumbers % 2;
                userNumbers = userNumbers / 2;
            }
            string output = "";
            foreach(int number in binary)
            {
                output += number;
            }
            return Convert.ToString(Convert.ToInt32(output, 2));
            ///Convert.ToInt32(string value, 2)     changes from binary to dec/// 8 > oct to dec/// 16> hex to dec
        }
        static void Main(string[] args)
        {
            Console.WriteLine(ReverseBinary(10)); //5
            Console.WriteLine(ReverseBinary(12)); ///3
            //Console.WriteLine(ReverseBinary(25)); ///19
            //Console.WriteLine(ReverseBinary(45)); /// now max = int 16 due to array lengt = 4
        }
    }
}