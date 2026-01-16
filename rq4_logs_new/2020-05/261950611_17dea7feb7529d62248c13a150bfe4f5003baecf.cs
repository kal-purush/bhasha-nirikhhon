using System;

namespace Leetcode
{
    class Program
    {
        public class Solution
        {
            public static bool IsOneBitCharacter(int[] bits)
            {
                int position = 0;
                for (int i = 0; i < bits.Length; i++)
                {
                    if (bits[i] == 0)
                    {
                        position = i;
                    }
                    else
                    {
                        position = i + 2;
                        i += 1;
                    }
                }
                if (bits.Length - position == 1)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }

            public static void Main(string[] args)
            {
                int[] test = new int[] { 1, 1, 0 };
                Console.WriteLine(IsOneBitCharacter(test));
            }
        }
    }
}