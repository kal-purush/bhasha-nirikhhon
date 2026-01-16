using System;
using System.Linq;

namespace AocNetLib
{
    public class Day1
    {
        public string Solve(string input)
        {
            int[] inputArr = ParseInput(input);
            int inc = 0;
            for (int i = 1; i < inputArr.Length; i++)
            {
                if (inputArr[i] > inputArr[i - 1]) inc++;
            }
            return inc.ToString();
        }

        public string Solve2(string input)
        {
            int[] inputArr = ParseInput(input);
            int inc = 0;
            int sum = inputArr[0] + inputArr[1] + inputArr[2];
            for (int i = 3; i < inputArr.Length; i++)
            {
                int sum2 = sum + inputArr[i] - inputArr[i - 3];
                if (sum2 > sum) inc++;
            }
            return inc.ToString();
        }

        private static int[] ParseInput(string input)
        {
            return input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x.Trim())).ToArray();
        }
    }
}