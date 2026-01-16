using System.Linq;
using System.Text.RegularExpressions;

namespace Advent2017.Solutions
{
    class Day9
    {
        private readonly Regex _escape = new Regex(@"\!.");
        private readonly Regex _garbage = new Regex(@"<.*?>");
       
        public int GetCountOfGroups(string input)
        {
            var escaped = _garbage.Replace(_escape.Replace(input, ""), "");
            return escaped.Count(character => character == '{');
        }

        public int GetTotalScore(string input)
        {
            var escaped = _garbage.Replace(_escape.Replace(input, ""), "");

            int score = 0;
            var nestCount = 0;
            foreach (var character in escaped)
            {
                switch (character)
                {
                    case '{':
                        score += ++nestCount;
                        break;
                    case '}':
                        nestCount--;
                        break;
                }
            }
            return score;
        }

        public int SizeOfGarbage(string input)
        {
            var matches = _garbage.Matches(_escape.Replace(input, ""));
            return matches.Cast<Match>().Sum(match => match.Length - 2);
        }
    }
}