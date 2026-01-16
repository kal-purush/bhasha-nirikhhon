using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Green
{
    //Write a function to find the longest common prefix string amongst an array of strings.
    //If there is no common prefix, return an empty string "".

    //Example 1:
    //Input: ["flower","flow","flight"]
    //Output: "fl"

    //Example 2:
    //Input: ["dog","racecar","car"]
    //Output: ""
    //Explanation: There is no common prefix among the input strings.

    //Note:
    //All given inputs are in lowercase letters a-z.

    class LongestCommonPrefix
    {
        public static void Solution()
        {
            Console.WriteLine("Enter words abstracted with \",\": ");
            var stringArrayFromUserInput = Console.ReadLine().Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).ToList();

            var trimmedInput = ReplaceWhiteSpaces(stringArrayFromUserInput);
            var lengthOfUserInputs = trimmedInput.Select(l => l.Length).ToList();

            var indexOfShortestInput = lengthOfUserInputs.FindIndex(l => l == lengthOfUserInputs.Min()); //predicate<int>
            var isEqualCounter = 0;

            for (int sequenceCut = lengthOfUserInputs.Min(); sequenceCut >= 1; sequenceCut--)
            {
                var cuttedString = trimmedInput[indexOfShortestInput].Take(sequenceCut);

                for (int item = 0; item < trimmedInput.Count; item++)
                {
                    if (trimmedInput[item].Take(sequenceCut).SequenceEqual(cuttedString)) //contains
                    {
                        isEqualCounter++;
                    }
                }

                if (isEqualCounter != trimmedInput.Count)
                {
                    isEqualCounter = 0;
                }
                else if (isEqualCounter == trimmedInput.Count)
                {
                    Console.WriteLine($"Longest common prefix is {new string(cuttedString.ToArray())}");
                    break;
                }
            }
            if (isEqualCounter != trimmedInput.Count)
            {
                Console.WriteLine("There is no common prefix among the input strings");
            }
            Console.ReadLine();
        }

        public static List<string> ReplaceWhiteSpaces(List<string> list)
        {
            var trimmedList = new List<string>();
            foreach (var item in list)
            {
                trimmedList.Add(item.Replace(" ", String.Empty));
            }
            return trimmedList;
        }
    }
}