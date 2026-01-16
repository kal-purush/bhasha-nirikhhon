using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace l1les21
{
    
    class Program
    {
        public static int MaxValue(int[] ArrInput,int begin,int end)
        {
            int max = 0;
            for (int i = begin; i <= end; i++)
            {
                if (max < ArrInput[i]) max = ArrInput[i];
            }
            return max;
        }

        public static int[] Transform(int[] TransInput)
        {
            //int[] TransRezult = new int[TransInput.Length];
            List <int> Temp = new List<int>();
            for (int i = 0; i < TransInput.Length; i++)
            {
                //int temp;
                for (int j = 0; j <TransInput.Length - i-1; j++)
                {
                    int k = i + j;
                  int  temp = MaxValue(TransInput, j, k);
                    Temp.Add(temp);
                }
             
            }
            int[] TransRezult = new int[Temp.Count];
            Temp.CopyTo(TransRezult);
            return TransRezult;
        }

        public static bool TransformTransform(int[] A, int N)
        {
            bool key=false;
            int[] temp = Transform(A);
            int[] rez = Transform(temp);
            int sum = 0;
            for (int i = 0; i < rez.Length; i++)
            {
                sum = sum + rez[i];
            }

            Console.WriteLine(sum);
            if (sum % 2 == 0) key = true;

            return key;


        }

        //static void Main(string[] args)
        //{
        //    int c = MaxValue(new int[]{ 11,0,35,5,6},0,1);
        //    Console.WriteLine(c);
        //    int[] test = Transform(new int[] { 2, 8, 4, 2, 6 });
        //    Console.WriteLine("1, 7, 3, 5, 6 ");
        //    foreach (int t in test) Console.Write(t +", ");
        //    Console.WriteLine();
        //    int[] test2 = Transform(test);
        //    foreach (int t in test2) Console.Write(t + " ");
        //    Console.WriteLine();
        //    bool d = TransformTransform(new int[] { 2, 8, 4, 2, 6 },5);
        //    Console.WriteLine(d);
        //}
    }
}