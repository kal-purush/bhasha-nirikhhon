using System.Linq;
using System.Text.RegularExpressions;

namespace Advent2015.Solutions
{
    class Day8
    {

        public int DecodedLength(string line)
        {
            var match = Regex.Match(line, @"^""(\\x..|\\.|.)*""$");
            return match.Groups[1].Captures.Count; // how many encoded things have we matched?
        }

        public int EncodedLength(string line)
        {
            return line.Replace("\\", "**").Replace("\"", "**").Length + 2;
        }

        public int Part1(string[] input)
        {
            return input.Sum(x => x.Length)- input.Sum(DecodedLength);
        }

        public int Part2(string[] input)
        {
            return input.Sum(EncodedLength) - input.Sum(x => x.Length);
        }
    }
}