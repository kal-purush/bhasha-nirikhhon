using System.Linq;

namespace SQLSkaner.IKeyWord
{
    class Integers : IKeyWords
    {

        public bool IsFullMatch(string input)
        {
            return input.All(char.IsDigit);
        }

        public bool IsPartialMatch(string input)
        {
            return false;
        }

        public string KeyWordName()
        {
            return "Integer";
        }

        public string WrapToHtml(string elementToBeWrapped)
        {
            return "<font style=\"color: Red\">" + elementToBeWrapped +"</font>";
        }
    }
}