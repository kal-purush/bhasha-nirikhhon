using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSExercises
{
    public class ex44entension
    {
        public static void Main(string[] args)
        {
            //Random rnd=new Random();
            //int x =rnd.Next(1,100);
            int remain;
            int index = 0;
            //Console.Write("the random number is " + x + "\n");
            int[] num = new int[20];
            for (int i = 1; i <= 100; i++)
            {
                index = 0;
                Console.Write(i + "\t");
                remain = i;
                while (remain >= 1)
                {
                    num[index] = remain % 2;
                    remain = remain / 2;
                    if (remain == 0)
                        break;
                    index = index + 1;
                }
                for (int j = index; j >= 0; j--)
                {
                    Console.Write(num[j]);
                }
                Console.WriteLine();
            }
      
        }
    }
}