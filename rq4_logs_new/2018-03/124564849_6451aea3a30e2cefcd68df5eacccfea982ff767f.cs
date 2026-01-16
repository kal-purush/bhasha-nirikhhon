using System.Linq;

namespace SQLSkaner.IKeyWord
{
    class Strings : IKeyWords
    {
        public bool IsFullMatch(string input)
        {
            var len = input.Length;
            if (input == null || len < 2) return false;
            if (input[0] != '"' || input[len - 1] != '"') return false;

            for (var i = 1; i < len - 2; i++)
            {
                if (input[i] == '"') return false;
            }

            return true;
        }

        public bool IsPartialMatch(string input)
        {

            if (string.IsNullOrEmpty(input)) return false;
            if (input.Length == 1 && input[0] == '"') return true;
            if (input[0] != '"') return false;

            for (var i = 1; i < input.Length; i++)
            {
                if (input[i] == '"') return false;
            }

            return true;
        }

        public string KeyWordName()
        {
            return "Strings";
        }

        public string WrapToHtml(string elementToBeWrapped)
        {
            return "<font style=\"color: Tomato\">" + elementToBeWrapped + "</font>";
        }
    }
}