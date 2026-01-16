using System;
using System.Collections.Generic;
using System.Linq;

namespace Range
{
    public static class Range
    {
        /*  3, 3, 5, 4, 10, 11, 12
             *  3, 3, 4, 5, 7, 8, 10, 11, 12
             *  min = 3
             *  3, 4, 5, 6, 7, 8, 9, 10, 11, 12
             *  
             *  6, 9
             *  step1: SortedArray = order array
             *  step2: generate Sequence number by providing min and max value of SortedArray
             *  Step3: 
             * 
             */

        public static List<List<int>> RangeDetector(int[] intarray)
        {
            intarray = intarray.OrderBy(x => x).ToArray();
            var sequence = SequenceGenerator(intarray.Min(), intarray.Max());
            var NonIntersectedNumbers = sequence.Except(intarray).ToArray();
            
            List<List<int>> lstRange = new List<List<int>>();
            List<int> lst;
            int arrayIndex = 0;
            for (int i = 0; i < NonIntersectedNumbers.Length; i++)
            {
                lst = new List<int>();
                for (int j = arrayIndex; j < intarray.Length; j++)
                {
                    if (intarray[j] < NonIntersectedNumbers[i])
                    {
                        lst.Add(intarray[j]);
                        continue;
                    }
                    arrayIndex = j;
                    break;
                }
                if (lst.Count > 0)
                    lstRange.Add(lst);
            }

            int cnt = 0;
            for (int i = 0; i < lstRange.Count; i++)
                cnt += lstRange[i].Count;

            lst = new List<int>();
            for (int j = cnt; j < intarray.Length; j++)
                lst.Add(intarray[j]);
            lstRange.Add(lst);

            return lstRange;
        }
        public static int[] SequenceGenerator(int min, int max)
        {
            int[] intarray = new int[max - min + 1];
            int index = 0;
            for (int i = min; i <= max; i++)
            {
                intarray[index] = i;
                index++;
            }
            return intarray;
        }

        public static void PrintRanges(this List<List<int>> range)
        {
            foreach (var data in range)
            {
                Console.WriteLine("{0}-{1},{2}", data.Min(), data.Max(), data.Count);
            }
        }
    }
}