using System;

namespace InsertShiftArray
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Insert Shift Array!");
        }
        public static int[] InsertShiftArray(int[] array, int value)
        {

            int[] solutionArray = new int[array.Length + 1];
            bool flag = false;

            for (int counter = 0; counter < solutionArray.Length; counter++)
            {
                if (flag == true)
                {
                    solutionArray[counter] = array[counter - 1];
                    continue;
                }
                if (counter == array.Length / 2)
                {
                    solutionArray[counter] = value;
                    flag = true;
                    continue;
                }
                solutionArray[counter] = array[counter];
            }
            foreach (int num in solutionArray)
            {
                Console.WriteLine(num);
            }
            return solutionArray;
        }
    }
}