namespace AocNetLib
{
    public class Day10
    {
        public string Solve(string input)
        {
            var file = ParseInput(input);
            return GetCorruptedScore(file).ToString();
        }

        private int GetCorruptedScore(string[] file)
        {
            return file.Sum(GetCorruptedScore);
        }

        private int GetCorruptedScore(string x)
        {
            Stack<char> opened = new Stack<char>();
            for (int i = 0; i < x.Length; i++)
            {
                char c = x[i];
                switch (c)
                {
                    case '(': opened.Push(')'); break;
                    case '[': opened.Push(']'); break;
                    case '{': opened.Push('}'); break;
                    case '<': opened.Push('>'); break;
                    case ')':
                    case ']':
                    case '}':
                    case '>':
                        char expected = opened.Pop();
                        if (c != expected) return GetCorruptedScore(c);
                        break;
                    default: throw new ArgumentException("Invalid character");
                }
            }
            return 0;
        }

        private int GetCorruptedScore(char c)
        {
            switch (c)
            {
                case ')': return 3;
                case ']': return 57;
                case '}': return 1197;
                case '>': return 25137;
            }
            throw new ArgumentException("Invalid character");
        }

        public string Solve2(string input)
        {
            var file = ParseInput(input);
            return GetIncompleteScore(file).ToString();
        }

        private long GetIncompleteScore(string[] file)
        {
            var scores = file.Select(GetIncompleteScore).Where(x => x != 0);
            return scores.OrderBy(x => x).ElementAt(scores.Count() / 2);
        }

        private long GetIncompleteScore(string x)
        {
            Stack<char> opened = new Stack<char>();
            for (int i = 0; i < x.Length; i++)
            {
                char c = x[i];
                switch (c)
                {
                    case '(': opened.Push(')'); break;
                    case '[': opened.Push(']'); break;
                    case '{': opened.Push('}'); break;
                    case '<': opened.Push('>'); break;
                    case ')':
                    case ']':
                    case '}':
                    case '>':
                        char expected = opened.Pop();
                        if (c != expected) return 0;
                        break;
                    default: throw new ArgumentException("Invalid character");
                }
            }
            long sum = 0;
            foreach (var item in opened)
            {
                sum = sum * 5 + GetIncompleteScore(item);
            }
            return sum;
        }

        private int GetIncompleteScore(char c)
        {
            switch (c)
            {
                case ')': return 1;
                case ']': return 2;
                case '}': return 3;
                case '>': return 4;
            }
            throw new ArgumentException("Invalid character");
        }

        private string[] ParseInput(string input)
        {
            return input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
        }
    }
}