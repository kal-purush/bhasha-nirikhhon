using System;
using System.Collections.Generic;
using System.Text;

namespace LeetCodeSolutions
{
    public static class Problem135
    {
        static public int Candy(int[] ratings)
        {
            int numChildren = ratings.Length;
            if (numChildren == 1)
            {
                return 1;
            }

            //Two pass algorithm, once from left and once from the right
            //O(n) in time and memory

            int[] CandiesLeft = new int[numChildren];
            int prevRating = ratings[0];
            CandiesLeft[0] = 1;

            for (int ii = 1; ii < numChildren; ii++)
            {
                int currentRating = ratings[ii];
                if (currentRating > prevRating)
                {
                    CandiesLeft[ii] = CandiesLeft[ii - 1] + 1;
                }
                else
                {
                    CandiesLeft[ii] = 1;
                }
                prevRating = currentRating;
            }

            prevRating = ratings[numChildren-1];
            int[] CandiesRight = new int[numChildren];
            CandiesRight[numChildren-1] = 1;

            for (int ii = numChildren - 2; ii >= 0; ii--)
            {
                int currentRating = ratings[ii];
                if (currentRating > prevRating)
                {
                    CandiesRight[ii] = CandiesRight[ii + 1] + 1;
                }
                else
                {
                    CandiesRight[ii] = 1;
                }
                prevRating = currentRating;
            }

            int answer = 0;
            for (int ii = 0; ii < numChildren; ii++)
            {
                answer += Math.Max(CandiesLeft[ii], CandiesRight[ii]);
            }

            return answer;
        }
    }
}