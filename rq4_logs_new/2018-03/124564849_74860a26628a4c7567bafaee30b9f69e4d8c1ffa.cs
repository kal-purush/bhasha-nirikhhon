
using System.Linq;

namespace SQLSkaner.IKeyWord
{
    class Floats : IKeyWords
    {

        public bool IsFullMatch(string input)
        {
            string[] splitInput = input.Split('.');

            if (splitInput.Length != 2 || splitInput[0] == null || splitInput[1] == null)
                return false;

            if ((splitInput[0].All(char.IsDigit)) &&
                (splitInput[1].All(char.IsDigit)))
                return true;

            return false;
        }

        public bool IsPartialMatch(string input)
        {
            return false;
        }

        public string KeyWordName()
        {
            return "Float";
        }

        public string WrapToHtml(string elementToBeWrapped)
        {
            return "<font style=\"color: Blue\">" + elementToBeWrapped + "</font>";
        }
    }
}