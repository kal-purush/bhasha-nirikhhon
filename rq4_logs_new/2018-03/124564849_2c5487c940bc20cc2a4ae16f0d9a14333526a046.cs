namespace SQLSkaner.IKeyWord
{
    class Dot : IKeyWords
    {
        public bool IsFullMatch(string input)
        {
            return (input == ".");
        }

        public bool IsPartialMatch(string input)
        {
            return false;
        }

        public string KeyWordName()
        {
            return "Dot";
        }

        public string WrapToHtml(string elementToBeWrapped)
        {
            return "<font style=\"color: Red\">" + elementToBeWrapped + "</font>";
        }
    }
}