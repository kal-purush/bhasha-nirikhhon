using System;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace TOCalculator
{
    public class Adder : IR365Calculator
    {
        private int[] _operands = { 0 };
        private int _maxVal;
        private string[] _delimiters;
        private char _delimiterTrimChar;
        private string _delimiterAnyLength;
        private bool _allowNegatives;

        public int MaxVal
        {
            get => _maxVal;
        }
        public string[] Delimiters
        {
            get => _delimiters;
            set => _delimiters = value;
        }
        public char DelimiterTrimChar
        {
            get => _delimiterTrimChar;
        }
        public string DelimiterAnyLength
        {
            get => _delimiterAnyLength;
        }
        public bool AllowNegatives
        {
            get => _allowNegatives;
        }

        public Adder(
            int maxVal,
            char delimiterTrimChar,
            string delimiterAnyLength,
            bool allowNegatives,
            string[] delimiters)
        {
            _maxVal = maxVal;
            _delimiters = delimiters;
            _delimiterTrimChar = delimiterTrimChar;
            _delimiterAnyLength = delimiterAnyLength;
            _allowNegatives = allowNegatives;
        }
        public void ResetDelimiters(string[] delimiters)
        {
            _delimiters = delimiters;
        }

        public void ParseInput(string customDelimiter, string input)
        {
            //check for valid input
            if (string.IsNullOrEmpty(input))
                input = "0";

            //remove any white space
            input.Replace(" ", "");

            //add custom delimiter
            MatchCollection matches = Regex.Matches(customDelimiter, @"\[([^]]*)\]");
            if (matches.Count == 0)
            {
                //delimiter is only a single character
                _delimiters = _delimiters.Append(customDelimiter.TrimStart(DelimiterTrimChar)).ToArray();
            }
            else //string delimiter(s)
            {
                foreach (Match m in matches)
                {
                    _delimiters = _delimiters.Append(m.Groups[1].Value).ToArray();
                }
            }

            //parse input on delimeters
            string[] stringArgs = input.Split(_delimiters, StringSplitOptions.None);

            //_addends = Array.ConvertAll(stringArgs, 
            //    s => (int.TryParse(s, out int intResult) ? intResult : 0))  //replace invalid, null or empty string with 0
            //    .Where(t => t <= _maxVal).ToArray();                        //filter 'invalid' numbers > _maxVal

            //refactor to allow replacement to 0 rather than filtering
            _operands = Array.ConvertAll(stringArgs,
                s => (int.TryParse(s, out int intResult) ? (intResult > MaxVal ? 0 : intResult) : 0)).ToArray();  //replace invalid, > _maxVal, null or empty string with 0 

            if (!_allowNegatives)
            {
                if (_operands.Any(t => t < 0))
                {
                    StringBuilder sb = new StringBuilder();
                    _operands.Where(t => t < 0).ToList().ForEach(t => sb.Append(t.ToString() + ","));
                    throw new ArgumentException("Negative arguments are not allowed! Invalid arguments are: " + sb.ToString().TrimEnd(','));
                }
            }
        }

        public string Calculate()
        {
            StringBuilder sb = new StringBuilder();
            int sum = 0;
            foreach (int i in _operands)
            {
                sum += i;
                sb.Append(i.ToString() + "+");
            }
            sb.Remove(sb.Length - 1, 1) //remove last '+'
                .Append(" = " + sum.ToString());

            return sb.ToString();
        }
    }
}