public class BracketValidation
{
    public static bool ValidateBrackets(string input)
    {
        Stack<char> stack = new Stack<char>();
        Queue<char> queue = new Queue<char>();

        foreach (char c in input)
        {
            if (c == '(' || c == '[' || c == '{')
            {
                stack.Push(c);
                queue.Enqueue(c);
            }
            else if (c == ')' || c == ']' || c == '}')
            {
                if (stack.Count == 0 || queue.Count == 0)
                    return false;

                char openBracket = stack.Pop();
                char correspondingOpenBracket = GetCorrespondingOpenBracket(c);

                if (openBracket != correspondingOpenBracket)
                    return false;

                queue.Dequeue();
            }
        }

        return stack.Count == 0 && queue.Count == 0;
    }

    private static char GetCorrespondingOpenBracket(char closingBracket)
    {
        switch (closingBracket)
        {
            case ')':
                return '(';
            case ']':
                return '[';
            case '}':
                return '{';
            default:
                throw new ArgumentException("Invalid closing bracket");
        }
    }
}
