using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class MyBigInteger
    {
        public int[] BigInt { get; set; }
        public MyBigInteger(int[] number)
        {
            BigInt = number;
        }

        public int[] Add(int[] number)
        {
            int helper = 0;
            List<int> resultList = new List<int>();
            List<int> numberList = number.ToList();
            List<int> BigIntList = BigInt.ToList();

            if (BigInt.Length > number.Length)
            {
                for (int i = numberList.Count; i < BigIntList.Count; i++)
                {
                    numberList.Add(0);
                }
            }
            else if (BigInt.Length < number.Length)
            {
                for (int i = BigIntList.Count; i < numberList.Count; i++)
                {
                    BigIntList.Add(0);
                }
            }

            for (int i = 0, j = 0; i < numberList.Count; i++, j++)
            {
                if (BigIntList[i] + numberList[j] - 10 + helper >= 0)
                {
                    resultList.Add(BigIntList[i] + numberList[j] + helper - 10);
                    helper = 1;
                }
                else
                {
                    resultList.Add(BigIntList[i] + numberList[j]);
                    helper = 0;
                }
            }

            if (helper == 1)
            {
                resultList.Add(helper);
            }

            int[] resultArray = new int[resultList.Count];
            resultList.Reverse();
            resultArray = resultList.ToArray();
            return resultArray;

        }
    }
}