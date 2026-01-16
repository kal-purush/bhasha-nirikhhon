using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLSkaner
{
    class PartialMatching
    {
        public static bool IsPartialMatchToRegex(string regex, string input)
        {
            input = input.ToUpper();
            regex = regex.ToUpper();

            if (input.Length >= regex.Length || input == "")
                return false;

            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == regex[i])
                    continue;
                if (i == 0)
                    return false;
            }
            return true;
        }
    }
}