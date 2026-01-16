using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Classwork_2_2
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Random random = new Random();
            int[] array =  new int[6];
            array[1] = (int)random.Next(1,5);
            Console.WriteLine(array[1]);
            Console.WriteLine(array.Length);

            for (int i = 0; i < array.Length; i++)
            {
                array[i]=random.Next(1,5);
            }


            for (int i = 0; i < array.Length; i++)
            {
                Console.Write(array[i] + " ");
            }

            /*  int i = 0;
              for (; ;)
              {
                  i++;
              }*/
            Console.WriteLine("foreach");
            foreach (int i in array)
            {
                Console.WriteLine(i + " ");
            }
          
            Console.WriteLine("dowhile") ;
            int j = 2;
            do
            {
                Console.WriteLine( j + " ");
            }
            while (j-- > 0);
           

            Console.WriteLine("while");
            j = 2;
            while (j-- >= 0) {
                Console.WriteLine(j + " iteration ");
            }

            Console.WriteLine("Continue and breake" );

            for (int i = 0; i < array.Length; i++)
            {
                if (i == 4) { break;}
                if (i == 1) { continue; }
                array[i] = (int)random.Next(1, 5);
                Console.Write(array[i] + " ");
            }

            // Console.WriteLine("\n task 1");
            // Task1();

            // Console.WriteLine("\n task 2");
            // Task2();

            Console.WriteLine("\n task 3");
            Task3();
            Console.WriteLine("\n task 4");
            //Task4();
            Console.WriteLine("\n task 5");
            //Task5();
            Console.WriteLine("\n task 7");
           // Task7();

            Console.ReadLine();

        }

        private static void Task7()
        {
            int[,] matrix =
            {
                {1,2,3},
                {1,2,3},
                {1,2,3} };

             Array[] matrixtooth =
            {
              new int[] {1,2,3},
              new string[]  {"","","",""},
              new int[]   {1,2,3,4,5} };

            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    Console.Write(matrix[i, j] + " ");
                }
            }
            Console.WriteLine("\n next matrix");

            for (int i = 0; i < matrix.GetLength(0); i++)
            {
                for (int j = 0; j < matrix.GetLength(1); j++)
                {
                    Console.Write(matrix[i, j] + " ");
                }
                Console.WriteLine();
            }
            Console.WriteLine("\n next matrix 2");

            Random random = new Random();
            int[,] matrix2 = new int[3, 3];

            for (int i = 0; i < matrix2.GetLength(0); i++)
            {
                for (int j = 0; j < matrix2.GetLength(1); j++)
                {
                    matrix2[i, j] = random.Next(1, 50);
                    Console.Write(matrix2[i, j] + " ");
                }
                Console.WriteLine();

            }

            Console.WriteLine("\n next matrix 3");


        }
        private static void Task6()
        {
            Random random = new Random();
            int[] array1 = new int[6];
            double result = 0;

            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 100);
                Console.Write(array1[i] + " ");
            }
            int maxnumb =  array1[0];

            int number = int.Parse(Console.ReadLine());
            for (int i = 0; i < array1.Length; i++)
            {
                if (array1[i] == number)
                {
                    break;
                }
                Console.WriteLine("arrai" + array1[i]);
            }
        }


        private static void Task5()
        {
            Random random = new Random();
            int[] array1 = new int[6];
            double result = 0;

            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 100);
                Console.Write(array1[i] + " ");
            }
            int maxnumb = array1[0];

            for (int i =0;i<array1.Length; i++)
            {
                if (maxnumb < array1[i])
                {
                    maxnumb = array1[i];
                }
            }
            Console.WriteLine("___________");
            Console.WriteLine(maxnumb + " maxnumb");
        }

        private static void Task4()
        {
            Random random = new Random();
            double[] array1 = new double[6];
            double result = 0.0;

            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 5);
                Console.Write(array1[i] + " ");
            }

            for (int i = array1.Length-1; i>=0; i--)
            {
                result = + array1[i];
                Console.Write("last " + array1[i]);
            }

        }

        private static void Task3()
        {
            Random random = new Random();
            double[] array1 = new double[6];
            double result = 0;

            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 5);
                Console.WriteLine(array1[i] + " ");
            }

            for (int i = 0; i < array1.Length; i++)
            {
                result += array1[i];
                Console.WriteLine(result + " elem");
            }
            Console.WriteLine(result);

            double avarage = array1.Length;
            Console.WriteLine(avarage);
            Console.WriteLine("result " + result / avarage);



        }

        private static void Task2()
        {
            Random random = new Random();
            int[] array1 = new int[6];
            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 5);
            }
            int result = 0;
            for (int i = 0; i < array1.Length; i++)
            {
                Console.WriteLine(array1[i]);
                if (array1[i] % 2 == 0)
                {
                   result++;
                }
            }
            Console.WriteLine("result=" + result);

        }

        private static void Task1()
        {
            Random random = new Random();
            int[] array1 = new int[6];

            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 5);
            }
            int result = 0;
            for (int i = 0; i < array1.Length; i++)
            {
                result += array1[i];
            }
            Console.WriteLine(result);

        }
    }
}