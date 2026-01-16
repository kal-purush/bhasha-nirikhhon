using challenges_and_data_structures1.MaxValue;
using System;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace ConsoleApp11
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // int[] findMax = new int[5] { 15, 35, 60, 80, 16 };
            // Console.WriteLine($"The maximmum number is: {MaximumValue.FindMaximum(findMax)}");

            // int[] Array = new int[5] { 1, 2, 3, 4, 5 };
            // saja(Array);


            // int[] Array2 = new int[12] { 4, 4, 4, 2, 1, 2, 2, 3, 3, 3, 5, 5 };
            // int mostFrequentNumber = saja2(Array2);

            //Console.WriteLine("Most Frequent Number: " + mostFrequentNumber);
            int[] input = { 1, 2, 3, 1, 2, 3 };
            int[] output = FindDuplicates(input);
            Console.WriteLine("Duplicates: " + string.Join(", ", output));

        }

        //    private static int[] saja(int[] Array)
        //    {

        //        int[] reverseArr = new int[Array.Length];
        //        for (int i = 0, y = Array.Length - 1; i < reverseArr.Length; i++, y--)
        //        {
        //            reverseArr[i] = Array[y];
        //        }
        //        for (int i = 0; i < reverseArr.Length; i++)
        //        {
        //            Console.WriteLine(reverseArr[i]);
        //        }
        //        return reverseArr;
        //    }

        //    private static int saja2(int[] Array2)
        //    {

        //        int mostFrequent = Array2[0];
        //        int maxCount = 1;

        //        for (int i = 0; i < Array2.Length; i++)
        //        {
        //            int count = 1;
        //            for (int j = i + 1; j < Array2.Length; j++)
        //            {
        //                if (Array2[j] == Array2[i])
        //                {
        //                    count++;
        //                }
        //            }

        //            if (count > maxCount)
        //            {
        //                maxCount = count;
        //                mostFrequent = Array2[i];
        //            }
        //        }

        //        return mostFrequent;
        //    }
        //}

        public static int[] FindDuplicates(int[] array)
        {
            // Create an empty array to store duplicate values
            int[] duplicates = new int[array.Length];
            int duplicateCount = 0;

            for (int i = 0; i < array.Length; i++)
            {
                for (int j = i + 1; j < array.Length; j++)
                {
                   
                    if (array[i] == array[j])
                    {
                        
                        bool isDuplicate = false;
                        for (int k = 0; k < duplicateCount; k++)
                        {
                            if (duplicates[k] == array[i])
                            {
                                isDuplicate = true;
                                break;
                            }
                        }

                        
                        if (!isDuplicate)
                        {
                            duplicates[duplicateCount] = array[i];
                            duplicateCount++;
                        }
                        break;
                    }
                }
            }

            
            Array.Resize(ref duplicates, duplicateCount);
            return duplicates;

        }
    }
}